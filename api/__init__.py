"""Root API."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logfire

from keys import KEYS


# Initialize Logfire
logfire.configure(
    token=KEYS.Logfire.write_token,
    environment=KEYS.Logfire.environment,
    scrubbing=False
)


app = FastAPI(
    title="API NAME",
    description=(
        "API DESCRIPTION"
    )
)
logfire.instrument_fastapi(app, capture_headers=True)


# Add middleware to disable CORS and allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/", tags=["Root"])
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {
        "message": "Server alive and well."
    }
