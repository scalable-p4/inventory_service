from __future__ import annotations

from typing import Any

from pydantic import BaseSettings, PostgresDsn, RedisDsn, root_validator

from src.constants import Environment
import os
from dotenv import load_dotenv

class Config(BaseSettings):
    load_dotenv()
    DATABASE_URL_INVENTORY: PostgresDsn = os.getenv("DATABASE_URL_INVENTORY")
    print("hi Database")
    print(os.getenv("DATABASE_URL_INVENTORY"))
    REDIS_URL: RedisDsn

    # SITE_DOMAIN: str = "tanapon.com"
    SITE_DOMAIN: str = "127.0.0.1:3000"

    ENVIRONMENT: Environment = Environment.PRODUCTION

    SENTRY_DSN: str | None

    CORS_ORIGINS: list[str]
    CORS_ORIGINS_REGEX: str | None
    CORS_HEADERS: list[str]

    APP_VERSION: str = "1"

    @root_validator(skip_on_failure=True)
    def validate_sentry_non_local(cls, data: dict[str, Any]) -> dict[str, Any]:
        if data["ENVIRONMENT"].is_deployed and not data["SENTRY_DSN"]:
            raise ValueError("Sentry is not set")

        return data


settings = Config()

app_configs: dict[str, Any] = {"title": "App API"}
if settings.ENVIRONMENT.is_deployed:
    app_configs["root_path"] = f"/v{settings.APP_VERSION}"

if not settings.ENVIRONMENT.is_debug:
    app_configs["openapi_url"] = None  # hide docs
