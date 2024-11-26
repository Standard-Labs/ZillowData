"""Models for the keys."""
from pydantic import BaseModel
from typing import Literal


class LogfireModel(BaseModel):
    """
    Model for Logfire token.
    """
    write_token: str
    environment: Literal["prod", "qa"]


class ProviderNameModel(BaseModel):
    """
    Model for MongoDB credentials.
    """
    api_key: str


class Keys(BaseModel):
    """
    Model for all keys.
    """
    Logfire: LogfireModel
    Provider: ProviderNameModel
