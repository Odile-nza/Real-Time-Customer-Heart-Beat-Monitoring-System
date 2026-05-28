"""
Tests for the Real-Time Customer Heart Beat Monitoring pipeline.

Run with:
    python -m pytest tests/ -v
"""

import json
import sys
import os
import pytest

# Allow importing from sibling packages
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))

from generator import generate_heartbeat, CUSTOMER_IDS
from consumer import validate, classify, HEART_RATE_MIN, HEART_RATE_MAX, ANOMALY_LOW, ANOMALY_HIGH


# ── Generator tests ──────────────────────────────────────────────────────────

class TestGenerator:
    def test_record_has_required_fields(self):
        record = generate_heartbeat()
        assert "customer_id" in record
        assert "timestamp" in record
        assert "heart_rate" in record
        assert "name" in record

    def test_customer_id_in_known_list(self):
        for _ in range(50):
            record = generate_heartbeat()
            assert record["customer_id"] in CUSTOMER_IDS

    def test_heart_rate_in_expected_range(self):
        for _ in range(100):
            record = generate_heartbeat()
            assert 40 <= record["heart_rate"] <= 200

    def test_record_is_json_serialisable(self):
        record = generate_heartbeat()
        serialised = json.dumps(record)
        restored = json.loads(serialised)
        assert restored["customer_id"] == record["customer_id"]
        assert restored["heart_rate"] == record["heart_rate"]


# ── Validation tests ─────────────────────────────────────────────────────────

class TestValidation:
    def _base(self):
        return {"customer_id": "C001", "heart_rate": 75, "timestamp": "2024-01-01T00:00:00Z"}

    def test_valid_record_passes(self):
        valid, reason = validate(self._base())
        assert valid is True
        assert reason == "ok"

    def test_missing_customer_id_fails(self):
        rec = self._base()
        del rec["customer_id"]
        valid, reason = validate(rec)
        assert valid is False
        assert "customer_id" in reason

    def test_missing_heart_rate_fails(self):
        rec = self._base()
        del rec["heart_rate"]
        valid, reason = validate(rec)
        assert valid is False
        assert "heart_rate" in reason

    def test_heart_rate_below_min_fails(self):
        rec = {**self._base(), "heart_rate": HEART_RATE_MIN - 1}
        valid, _ = validate(rec)
        assert valid is False

    def test_heart_rate_above_max_fails(self):
        rec = {**self._base(), "heart_rate": HEART_RATE_MAX + 1}
        valid, _ = validate(rec)
        assert valid is False

    def test_boundary_min_passes(self):
        rec = {**self._base(), "heart_rate": HEART_RATE_MIN}
        valid, _ = validate(rec)
        assert valid is True

    def test_boundary_max_passes(self):
        rec = {**self._base(), "heart_rate": HEART_RATE_MAX}
        valid, _ = validate(rec)
        assert valid is True


# ── Anomaly classification tests ─────────────────────────────────────────────

class TestClassify:
    def test_normal_heart_rate(self):
        assert classify(75) == "normal"
        assert classify(ANOMALY_LOW) == "normal"
        assert classify(ANOMALY_HIGH) == "normal"

    def test_low_anomaly(self):
        assert classify(ANOMALY_LOW - 1) == "anomaly"
        assert classify(40) == "anomaly"

    def test_high_anomaly(self):
        assert classify(ANOMALY_HIGH + 1) == "anomaly"
        assert classify(200) == "anomaly"

    def test_boundary_below_low_is_anomaly(self):
        assert classify(ANOMALY_LOW - 1) == "anomaly"

    def test_boundary_above_high_is_anomaly(self):
        assert classify(ANOMALY_HIGH + 1) == "anomaly"


# ── Full record simulation (integration-style, no external services) ─────────

class TestSimulatedPipeline:
    def test_generated_records_pass_validation(self):
        """100 generated records should all pass the validator."""
        failures = []
        for _ in range(100):
            record = generate_heartbeat()
            valid, reason = validate(record)
            if not valid:
                failures.append((record, reason))
        assert failures == [], f"Validation failed for: {failures}"

    def test_generated_records_get_classified(self):
        """Every generated record gets a valid status label."""
        for _ in range(100):
            record = generate_heartbeat()
            status = classify(record["heart_rate"])
            assert status in ("normal", "anomaly")

    def test_anomaly_rate_is_plausible(self):
        """With heart_rate in [40,200], ~25% should be anomalies (< 50 or > 160)."""
        records = [generate_heartbeat() for _ in range(500)]
        anomalies = [r for r in records if classify(r["heart_rate"]) == "anomaly"]
        rate = len(anomalies) / len(records)
        # Anomaly range: [40,49] ∪ [161,200] = 10+40 = 50 out of 161 values → ~31%
        assert 0.10 < rate < 0.55, f"Unexpected anomaly rate: {rate:.2%}"
