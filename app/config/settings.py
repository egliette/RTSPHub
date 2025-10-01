from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Centralized application configuration."""

    # API / App metadata
    API_PREFIX: str = "/api"
    PROJECT_NAME: str = "RTSPHub"
    SERVICE_NAME: str = "rtsphub"
    API_TOKEN: str = ""  # If set, requests must provide X-API-Token header

    # Logging
    LOG_DIR: str = "logs"
    LOG_TO_STDOUT: bool = True
    LOG_LEVEL: str = "INFO"
    LOG_MAX_DAYS: int = 30

    # CORS
    FRONTEND_URL: str = "http://localhost:3000"
    ADMIN_PANEL_URL: str = "http://localhost:3001"

    @property
    def BACKEND_CORS_ORIGINS(self) -> List[str]:
        return [self.FRONTEND_URL, self.ADMIN_PANEL_URL]

    # RTSP / Streaming
    MEDIA_SERVER_HOST: str = Field(default="localhost")
    MEDIA_SERVER_PORT: int = Field(default=8554)
    MEDIA_USERNAME: str = Field(default="admin")
    MEDIA_PASSWORD: str = Field(default="admin123")
    RESTART_BACKOFF_SECONDS: int = Field(default=5, ge=1)
    FFMPEG_PATH: str = Field(default="ffmpeg")

    @property
    def media_server_rtsp_base_url(self) -> str:
        """Construct RTSP base URL with credentials."""
        return f"rtsp://{self.MEDIA_USERNAME}:{self.MEDIA_PASSWORD}@{self.MEDIA_SERVER_HOST}:{self.MEDIA_SERVER_PORT}"

    model_config = {"env_file": ".env"}


settings = Settings()
