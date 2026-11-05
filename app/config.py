"""Typed configuration, loaded from environment variables or a .env file.

Every setting has a default that works on a bare machine, so the service runs
with nothing installed but Python. Redis and Postgres are opt-in.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="FEED_",
        extra="ignore",
    )

    # --- storage -----------------------------------------------------------
    # SQLite by default so the service needs no database server to run.
    database_url: str = "sqlite:///./feed_ingest.db"

    # --- cache / lock ------------------------------------------------------
    # Disabled by default. When off, the cache is a no-op and the lock always
    # grants: single-instance behaviour, which is correct for local runs.
    redis_enabled: bool = False
    redis_url: str = "redis://localhost:6379/0"
    cache_ttl_seconds: int = 60
    lock_ttl_seconds: int = 300

    # --- ingestion ---------------------------------------------------------
    # Comma-separated feed URLs. Empty by default; the job then does nothing
    # rather than failing, which keeps a scheduled run harmless when
    # unconfigured.
    feed_urls: str = ""
    fetch_timeout_seconds: float = 10.0
    fetch_max_attempts: int = 3
    fetch_backoff_base_seconds: float = 0.5

    # --- scheduling --------------------------------------------------------
    schedule_minutes: int = 15
    run_on_startup: bool = False

    # --- webhook -----------------------------------------------------------
    webhook_secret: str = "change-me"

    @property
    def feed_url_list(self) -> list[str]:
        return [u.strip() for u in self.feed_urls.split(",") if u.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
