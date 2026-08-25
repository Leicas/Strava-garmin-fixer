from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Service
    public_base_url: str = "http://localhost:8000"
    database_path: Path = Path("data/state.db")

    # Strava
    strava_client_id: str = ""
    strava_client_secret: str = ""

    # Google Health (replaces the deprecated Fitbit Web API)
    google_client_id: str = ""
    google_client_secret: str = ""

    # Garmin Connect (unofficial API — account credentials, no dev program).
    # Since Strava paywalled its API (June 2026), Garmin Connect is the
    # primary replace target: merged FITs are uploaded here and Garmin's
    # native sync pushes them on to Strava.
    garmin_email: str = ""
    garmin_password: str = ""
    garmin_tokens_path: Path = Path("data/garmin_tokens")
    # Poller: checks Garmin for new activities every N minutes. 0 disables.
    garmin_poll_minutes: int = 0
    garmin_poll_mode: str = "auto"  # 'dry_run' | 'semi_auto' | 'auto'
    # Only auto-enqueue activities that started within this window.
    garmin_poll_lookback_hours: int = 48
    # Don't touch an activity until it is at least this old — gives the
    # Fitbit/Pixel watch time to sync its data to Google Health, so the merge
    # sees the HR source. Activities with no match after this age are
    # delivered to Dreeve as-is (passthrough).
    garmin_poll_min_age_minutes: int = 120

    # Dreeve hand-off: when enabled, the Garmin worker drops the definitive
    # FIT (merged when matched, original otherwise) into dreeve_export_dir.
    # When co-located with Dreeve, bind-mount Dreeve's watch folder there and
    # the daemon imports within 5 minutes; the /export HTTP API serves the
    # same directory as a remote fallback. IMPORTANT: Dreeve dedups on
    # (sport, start time) — the stock dreeve-garmin-connector's download loop
    # must be OFF or Dreeve imports the unmerged original first and skips the
    # merged file forever.
    dreeve_export_enabled: bool = False
    dreeve_export_dir: Path = Path("data/export")

    # Webhook
    verify_token: str = Field(default="change-me", min_length=1)
    # If set, the webhook handler drops events whose owner_id != this. Strava
    # webhook POSTs are not signed; this is the strongest filter available.
    strava_owner_id: int | None = None

    # Dashboard auth (HTTP Basic). Required unless dashboard_auth_disabled=True.
    dashboard_user: str = ""
    dashboard_password: str = ""
    # Local-dev escape hatch ONLY. Never set this on a publicly reachable host.
    dashboard_auth_disabled: bool = False

    @property
    def strava_redirect_uri(self) -> str:
        return f"{self.public_base_url.rstrip('/')}/auth/strava/callback"

    @property
    def google_redirect_uri(self) -> str:
        return f"{self.public_base_url.rstrip('/')}/auth/google/callback"


settings = Settings()
