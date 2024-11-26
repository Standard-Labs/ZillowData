"""Models for the keys."""
from pydantic import BaseModel
from typing import Literal


# class LogfireModel(BaseModel):
#     """
#     Model for Logfire token.
#     """
#     write_token: str
#     environment: Literal["prod", "qa"]



class ScraperAPIModel(BaseModel):
    """
    Model for ScraperAPI token.
    """
    api_key: str


class SupabaseModel(BaseModel):
    """
    Model for Supabase credentials.
    """
    url: str
    secret: str


class Keys(BaseModel):
    """
    Model for all keys.
    """
    # Logfire: LogfireModel
    ScraperAPI: ScraperAPIModel
    Supabase: SupabaseModel
