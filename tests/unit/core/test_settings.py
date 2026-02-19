from consist.core.settings import ConsistSettings


def test_settings_from_env_reads_lock_retry_overrides(monkeypatch) -> None:
    monkeypatch.setenv("CONSIST_DLT_LOCK_RETRIES", "31")
    monkeypatch.setenv("CONSIST_DLT_LOCK_BASE_SLEEP_SECONDS", "0.25")
    monkeypatch.setenv("CONSIST_DLT_LOCK_MAX_SLEEP_SECONDS", "4.5")
    monkeypatch.setenv("CONSIST_DB_LOCK_RETRIES", "29")
    monkeypatch.setenv("CONSIST_DB_LOCK_BASE_SLEEP_SECONDS", "0.15")
    monkeypatch.setenv("CONSIST_DB_LOCK_MAX_SLEEP_SECONDS", "3.5")

    settings = ConsistSettings.from_env()

    assert settings.dlt_lock_retries == 31
    assert settings.dlt_lock_base_sleep_seconds == 0.25
    assert settings.dlt_lock_max_sleep_seconds == 4.5
    assert settings.db_lock_retries == 29
    assert settings.db_lock_base_sleep_seconds == 0.15
    assert settings.db_lock_max_sleep_seconds == 3.5
