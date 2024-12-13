"""Models for the keys."""
from pydantic import BaseModel

class ScrapeWorkers(BaseModel):
    max_workers: int

class Config(BaseModel):
    """
    Model for all configuration.
    """
    ScrapeWorkers: ScrapeWorkers
