""" This module contains the FastAPI endpoints for querying the database. """
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from database.models import Agent as AgentModel, City as CityModel, Listing as ListingModel, AgentCity as AgentCityModel
from database.async_inserter import AsyncInserter
from typing import AsyncGenerator
from keys import KEYS

DATABASE_URL = f"postgresql+asyncpg://{KEYS.asyncpgCredentials.user}:{KEYS.asyncpgCredentials.password}@{KEYS.asyncpgCredentials.host}:{KEYS.asyncpgCredentials.port}/{KEYS.asyncpgCredentials.database}"
query_router = APIRouter()
async_inserter = AsyncInserter(DATABASE_URL)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_inserter.get_session() as session:
        yield session


# Two versions of the same endpoint, one using relationships and the other using regular joins
# First version utilizes joinedload for forcing eager loading of the cities relationship
# Second version uses regular joins through junction table, not taking advantage of sqlalchemy relationships
@query_router.get("/agent/{agent_id}/cities")
async def get_agent_cities(agent_id: str, session: AsyncSession = Depends(get_session)):
    """
    Get a list of all cities an agent is associated with.
    """
    result = await session.execute(
        select(AgentModel).options(joinedload(AgentModel.cities)).where(AgentModel.encodedzuid == agent_id)
    )
    agent = result.unique().scalar_one_or_none()
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    cities = [(city.city, city.state) for city in agent.cities]
    return cities


# # Through using regular joins through junction table, not taking advantage of sqlalchemy relationships
# @query_router.get("/agent/{agent_id}/cities")
# async def get_agent_cities(agent_id: str, session: AsyncSession = Depends(get_session)):
#     """
#     Get a list of all cities an agent is associated with.
#     """
#     result = await session.execute(
#         select(CityModel.city)  # Select only the city name from CityModel
#         .join(AgentCityModel, AgentCityModel.city_id == CityModel.id)  # Join AgentCityModel to CityModel
#         .join(AgentModel, AgentModel.encodedzuid == AgentCityModel.agent_id)  # Join AgentModel to AgentCityModel
#         .where(AgentModel.encodedzuid == agent_id)  # Filter by the agent ID
#     )

#     cities = [city[0] for city in result.fetchall()]

#     return cities