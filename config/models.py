"""Models for the keys."""
from pydantic import BaseModel


class GeneralConfig(BaseModel):
    """
    Model for general configuration.
    """


class Config(BaseModel):
    """
    Model for all configuration.
    """
    General: GeneralConfig
