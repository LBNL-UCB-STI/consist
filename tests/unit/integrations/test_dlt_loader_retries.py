from consist.integrations.dlt_loader import _is_retryable_duckdb_lock_error


def test_retryable_duckdb_lock_error_detects_conflicting_lock() -> None:
    err = Exception(
        'IO Error: Could not set lock on file "/tmp/provenance.duckdb": '
        "Conflicting lock is held in PID 0."
    )
    assert _is_retryable_duckdb_lock_error(err) is True


def test_retryable_duckdb_lock_error_rejects_unrelated_errors() -> None:
    err = Exception("constraint violation: duplicate key")
    assert _is_retryable_duckdb_lock_error(err) is False
