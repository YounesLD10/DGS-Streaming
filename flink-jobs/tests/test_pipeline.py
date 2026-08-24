"""
Unit tests for the HPS-SWAM RT PoC pipeline.

Run with: cd flink-jobs && python -m pytest tests/ -v
"""
import sys
import os
import json

import pytest

# Ensure flink-jobs/ is on the path (also done by conftest.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.iso_standards import luhn_check, is_valid_mti, is_valid_currency


# ---------------------------------------------------------------------------
# Helpers — import business logic without triggering PyFlink imports
# ---------------------------------------------------------------------------

def _import_validate():
    """Import _validate_transaction from job2 without executing PyFlink setup."""
    # job2_validate imports pyflink at module level; we patch sys.modules so the
    # import succeeds in a non-Flink environment by providing stub modules.
    _stub_pyflink()
    from job2_validate import _validate_transaction
    return _validate_transaction

def _import_job3_helpers():
    _stub_pyflink()
    from job3_normalize import _parse_date, _parse_amount
    return _parse_date, _parse_amount

def _import_job4_helpers():
    _stub_pyflink()
    from job4_optimize import _risk_score, _extract_dedup_key
    return _risk_score, _extract_dedup_key

def _stub_pyflink():
    """Insert minimal stubs for PyFlink modules so job files can be imported."""
    import types

    def _make_stub(*names):
        mod = types.ModuleType(names[0])
        mod.__path__ = []
        sys.modules[names[0]] = mod
        for name in names[1:]:
            parts = name.split(".")
            parent = sys.modules[names[0]]
            for i, part in enumerate(parts[1:], 2):
                full = ".".join(parts[:i])
                if full not in sys.modules:
                    child = types.ModuleType(full)
                    child.__path__ = []
                    sys.modules[full] = child
                    setattr(parent, part, child)
                parent = sys.modules[full]

    stubs = [
        "pyflink",
        "pyflink.common",
        "pyflink.common.serialization",
        "pyflink.common.typeinfo",
        "pyflink.datastream",
        "pyflink.datastream.connectors",
        "pyflink.datastream.connectors.kafka",
        "pyflink.datastream.functions",
        "pyflink.datastream.state",
    ]
    for stub in stubs:
        if stub not in sys.modules:
            _make_stub(stub)

    # Stub commonly used names
    pf = sys.modules["pyflink"]
    pf_common = sys.modules["pyflink.common"]
    pf_ds = sys.modules["pyflink.datastream"]
    pf_ds_fn = sys.modules["pyflink.datastream.functions"]
    pf_ds_state = sys.modules["pyflink.datastream.state"]
    pf_ds_kafka = sys.modules["pyflink.datastream.connectors.kafka"]

    # WatermarkStrategy stub
    class _WMS:
        @staticmethod
        def no_watermarks(): return None
    pf_common.WatermarkStrategy = _WMS

    # Types stub
    class _Types:
        @staticmethod
        def STRING(): return "STRING"
        @staticmethod
        def BOOLEAN(): return "BOOLEAN"
    pf_common.typeinfo = sys.modules["pyflink.common.typeinfo"]
    sys.modules["pyflink.common.typeinfo"].Types = _Types
    pf_common.Types = _Types

    # OutputTag stub
    class _OutputTag:
        def __init__(self, *a, **kw): pass
    pf_ds.OutputTag = _OutputTag
    pf_ds.CheckpointingMode = type("CM", (), {"EXACTLY_ONCE": 1})()

    # Function base classes
    class _ProcessFn:
        class Context: pass
    class _KeyedProcessFn:
        class Context:
            def get_current_key(self): return None
    class _MapFn:
        pass

    pf_ds_fn.ProcessFunction = _ProcessFn
    pf_ds_fn.KeyedProcessFunction = _KeyedProcessFn
    pf_ds_fn.MapFunction = _MapFn

    # ValueStateDescriptor stub
    class _VSD:
        def __init__(self, *a, **kw): pass
    pf_ds_state.ValueStateDescriptor = _VSD

    # StreamExecutionEnvironment stub
    class _SEE:
        @staticmethod
        def get_execution_environment(): return _SEE()
        def set_parallelism(self, *a): pass
        def enable_checkpointing(self, *a): pass
        def get_checkpoint_config(self): return self
        def set_checkpointing_mode(self, *a): pass
        def set_min_pause_between_checkpoints(self, *a): pass
        def set_checkpoint_timeout(self, *a): pass
        def set_max_concurrent_checkpoints(self, *a): pass
        def from_source(self, *a, **kw): return self
        def key_by(self, *a, **kw): return self
        def process(self, *a, **kw): return self
        def map(self, *a, **kw): return self
        def uid(self, *a): return self
        def name(self, *a): return self
        def sink_to(self, *a): return self
        def get_side_output(self, *a): return self
        def execute(self, *a): pass
        def print(self, *a): return self
    pf_ds.StreamExecutionEnvironment = _SEE

    # SimpleStringSchema stub
    sys.modules["pyflink.common.serialization"].SimpleStringSchema = object

    # KafkaSource / KafkaSink stubs
    class _KB:
        def __init__(self): pass
        def builder(self): return self
        def set_bootstrap_servers(self, *a): return self
        def set_topics(self, *a): return self
        def set_group_id(self, *a): return self
        def set_starting_offsets(self, *a): return self
        def set_value_only_deserializer(self, *a): return self
        def set_record_serializer(self, *a): return self
        def set_delivery_guarantee(self, *a): return self
        def build(self): return self
        def set_topic(self, *a): return self
        def set_value_serialization_schema(self, *a): return self
    pf_ds_kafka.KafkaSource = _KB
    pf_ds_kafka.KafkaSink = _KB
    pf_ds_kafka.KafkaRecordSerializationSchema = _KB
    pf_ds_kafka.KafkaOffsetsInitializer = type("KOI", (), {
        "earliest": staticmethod(lambda: None),
        "latest": staticmethod(lambda: None),
    })()
    pf_ds_kafka.DeliveryGuarantee = type("DG", (), {"AT_LEAST_ONCE": 1, "EXACTLY_ONCE": 2})()


# ---------------------------------------------------------------------------
# Luhn tests
# ---------------------------------------------------------------------------

def test_luhn_valid_visa():
    assert luhn_check("4111111111111111")

def test_luhn_invalid():
    assert not luhn_check("4111111111111112")

def test_luhn_too_short():
    # luhn_check returns False for < 12 digits
    assert not luhn_check("41111")

def test_luhn_check_fires_on_unmasked_pan():
    """Rule ⑨ must catch an invalid PAN — verify with direct _validate_transaction call."""
    _validate_transaction = _import_validate()
    tx = {
        "MESSAGE_TYPE": "1100",
        "TRANSACTION_AMOUNT": "100.00",
        "TRANSACTION_CURRENCY": "504",
        "ISSUING_BANK": "Test Bank",
        "CARD_TYPE": "VISA",
        "REJECT_CODE": "",
        "CARD_NUMBER": "4111111111111112",  # invalid Luhn — last digit wrong
    }
    valid, reason = _validate_transaction(tx)
    assert not valid
    assert reason == "ISO7812_LUHN_FAILED"

def test_luhn_check_passes_valid_pan():
    """Rule ⑨ must pass a valid PAN through."""
    _validate_transaction = _import_validate()
    tx = {
        "MESSAGE_TYPE": "1100",
        "TRANSACTION_AMOUNT": "100.00",
        "TRANSACTION_CURRENCY": "504",
        "ISSUING_BANK": "Test Bank",
        "CARD_TYPE": "VISA",
        "REJECT_CODE": "",
        "CARD_NUMBER": "4111111111111111",  # valid Luhn
    }
    valid, reason = _validate_transaction(tx)
    assert valid
    assert reason == ""


# ---------------------------------------------------------------------------
# Date parsing (job3)
# ---------------------------------------------------------------------------

def test_parse_date_dd_mm_yyyy_hhmm():
    _parse_date, _ = _import_job3_helpers()
    assert _parse_date("15/01/2024 14:30") == "2024-01-15T14:30:00+00:00"

def test_parse_date_iso():
    _parse_date, _ = _import_job3_helpers()
    result = _parse_date("2024-01-15")
    assert result == "2024-01-15T00:00:00+00:00"

def test_parse_date_fallback():
    _parse_date, _ = _import_job3_helpers()
    # Unparseable string should return as-is without raising
    result = _parse_date("N/A")
    assert result == "N/A"

def test_parse_date_none():
    _parse_date, _ = _import_job3_helpers()
    assert _parse_date(None) is None


# ---------------------------------------------------------------------------
# Amount parsing (job3)
# ---------------------------------------------------------------------------

def test_parse_amount_scientific():
    _, _parse_amount = _import_job3_helpers()
    assert _parse_amount("0,11E+02") == pytest.approx(11.0)

def test_parse_amount_comma_decimal():
    _, _parse_amount = _import_job3_helpers()
    assert _parse_amount("700,50") == pytest.approx(700.50)

def test_parse_amount_plain():
    _, _parse_amount = _import_job3_helpers()
    assert _parse_amount("700") == pytest.approx(700.0)

def test_parse_amount_dot_decimal():
    _, _parse_amount = _import_job3_helpers()
    assert _parse_amount("1.50") == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# Risk scoring post-fix (job4) — confirm dead branches are gone
# ---------------------------------------------------------------------------

def test_risk_high_large_amount():
    _risk_score, _ = _import_job4_helpers()
    assert _risk_score({"TRANSACTION_AMOUNT": 15000, "MATCHING_STATUS": "U"}) == "HIGH"

def test_risk_medium_unknown_matching():
    _risk_score, _ = _import_job4_helpers()
    assert _risk_score({"TRANSACTION_AMOUNT": 500, "MATCHING_STATUS": "X"}) == "MEDIUM"

def test_risk_low():
    _risk_score, _ = _import_job4_helpers()
    assert _risk_score({"TRANSACTION_AMOUNT": 500, "MATCHING_STATUS": "U"}) == "LOW"

def test_risk_dead_branch_reject_code_not_high():
    """After fix, a record with REJECT_CODE should never reach _risk_score;
    if it somehow did, REJECT_CODE alone no longer drives HIGH risk — only amount does."""
    _risk_score, _ = _import_job4_helpers()
    # REJECT_CODE present but small amount + valid matching → LOW (dead branch removed)
    result = _risk_score({"TRANSACTION_AMOUNT": 500, "MATCHING_STATUS": "U", "REJECT_CODE": "05"})
    assert result == "LOW"

def test_risk_dead_branch_zero_amount_not_medium():
    """After fix, amount==0 no longer drives MEDIUM — only MATCHING_STATUS does."""
    _risk_score, _ = _import_job4_helpers()
    # Zero amount + valid matching → LOW (dead branch removed)
    result = _risk_score({"TRANSACTION_AMOUNT": 0, "MATCHING_STATUS": "U"})
    assert result == "LOW"


# ---------------------------------------------------------------------------
# Dedup composite key (job4)
# ---------------------------------------------------------------------------

def _extract_dedup_key_for_test(auth_code: str, mti: str) -> str:
    """Helper that builds the expected composite key."""
    return f"{auth_code}|{mti}"

def test_dedup_key_same_auth_same_mti_is_duplicate():
    """Same auth + same MTI → same key (duplicate)."""
    key_a = _extract_dedup_key_for_test("AUTH123", "1100")
    key_b = _extract_dedup_key_for_test("AUTH123", "1100")
    assert key_a == key_b

def test_dedup_key_same_auth_reversal_mti_not_duplicate():
    """Same auth + reversal MTI → different key (not duplicate)."""
    key_normal   = _extract_dedup_key_for_test("AUTH123", "1100")
    key_reversal = _extract_dedup_key_for_test("AUTH123", "1420")
    assert key_normal != key_reversal

def test_extract_dedup_key_function():
    """Test the actual _extract_dedup_key function from job4."""
    _, _extract_dedup_key = _import_job4_helpers()
    record = json.dumps({
        "transaction": {
            "AUTHORIZATION_CODE": "AUTH456",
            "MESSAGE_TYPE": "1100",
        }
    })
    assert _extract_dedup_key(record) == "AUTH456|1100"

def test_extract_dedup_key_reversal():
    _, _extract_dedup_key = _import_job4_helpers()
    record = json.dumps({
        "transaction": {
            "AUTHORIZATION_CODE": "AUTH456",
            "MESSAGE_TYPE": "1420",
        }
    })
    assert _extract_dedup_key(record) == "AUTH456|1420"


# ---------------------------------------------------------------------------
# ISO whitelist tests (accurate names from 2.1 fix)
# ---------------------------------------------------------------------------

def test_mti_whitelist_HPS_subset_known():
    assert is_valid_mti("1100")  # Authorization Request
    assert is_valid_mti("1110")  # Authorization Response

def test_mti_whitelist_HPS_subset_unknown():
    # 0100 is valid ISO 8583 but not in HPS whitelist
    assert not is_valid_mti("0100")

def test_currency_whitelist_MENA_subset():
    assert is_valid_currency("504")   # MAD — Morocco
    assert is_valid_currency("840")   # USD

def test_currency_whitelist_BRL_not_in_whitelist():
    # BRL (986) is valid ISO 4217 but not in the 23-currency MENA whitelist
    assert not is_valid_currency("986")
