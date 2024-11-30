"""Models for the keys."""
from pydantic import BaseModel
from typing import Literal


class LogfireModel(BaseModel):
    """
    Model for Logfire token.
    """
    write_token: str
    environment: Literal["prod", "qa"]


class ScraperAPIModel(BaseModel):
    """
    Model for ScraperAPI token.
    """
    api_key: str


class asyncpgModel(BaseModel):
    """
    Model for asyncpg credentials.
    """
    user: str
    password: str
    database: str
    host: str
    port: int


class Keys(BaseModel):
    """
    Model for all keys.
    """
    Logfire: LogfireModel
    ScraperAPI: ScraperAPIModel
    asyncpgCredentials: asyncpgModel
