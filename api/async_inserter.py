from keys import KEYS
from database.async_inserter import AsyncInserter

DATABASE_URL = f"postgresql+asyncpg://{KEYS.asyncpgCredentials.user}:{KEYS.asyncpgCredentials.password}@{KEYS.asyncpgCredentials.host}:{KEYS.asyncpgCredentials.port}/{KEYS.asyncpgCredentials.database}"
async_inserter = AsyncInserter(DATABASE_URL)
