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
