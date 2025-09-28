from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_PREFIX: str = "/api"
    PROJECT_NAME: str = ""

    # Logging settings
    LOG_DIR: str = "logs"
    LOG_TO_STDOUT: bool = True
    LOG_LEVEL: str = "INFO"
    LOG_MAX_DAYS: int = 30
    SERVICE_NAME: str = ""

    @property
    def BACKEND_CORS_ORIGINS(self) -> List[str]:
        """Build CORS origins list from individual URLs"""
        return [self.FRONTEND_URL, self.ADMIN_PANEL_URL]

    model_config = {"env_file": ".env"}


settings = Settings()
