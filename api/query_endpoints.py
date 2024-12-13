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


@query_router.get("/agentIDs/by_city_state/{city}/{state}")
async def get_agents_by_city_state(city: str, state: str, session: AsyncSession = Depends(get_session)):
    """
    Get a list of all agent IDs associated with a specific city and state.
    """
    result = await session.execute(
        select(AgentModel.encodedzuid).join(AgentCityModel).join(CityModel).where(
            CityModel.city == city.upper(),
            CityModel.state == state.upper()
        )
    )
    agent_ids = [row[0] for row in result.fetchall()]
    if not agent_ids:
        raise HTTPException(status_code=404, detail="No agents found for the specified city and state")
    
    return agent_ids


@query_router.get("/agent/{agent_id}")
async def get_agent(agent_id: str, session: AsyncSession = Depends(get_session)):
    """
    Get an agent object by their encodedzuid.
    """
    result = await session.execute(select(AgentModel).where(AgentModel.encodedzuid == agent_id))
    agent = result.unique().scalar_one_or_none()
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    return agent

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

@query_router.get("/agent/{agent_id}/phones")
async def get_agent_phones(agent_id: str, session: AsyncSession = Depends(get_session)):
    """
    Get a list of all phone numbers associated with an agent.
    """
    result = await session.execute(
        select(AgentModel).options(joinedload(AgentModel.phones)).where(AgentModel.encodedzuid == agent_id)
    )
    agent = result.unique().scalar_one_or_none()
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    phones = [{"phone": phone.phone, "type": phone.type} for phone in agent.phones]
    return phones

@query_router.get("/agent/{agent_id}/websites")
async def get_agent_websites(agent_id: str, session: AsyncSession = Depends(get_session)):
    """
    Get a list of all websites associated with an agent.
    """
    result = await session.execute(
        select(AgentModel).options(joinedload(AgentModel.websites)).where(AgentModel.encodedzuid == agent_id)
    )
    agent = result.unique().scalar_one_or_none()
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    websites = [{"website_url": website.website_url, "website_type": website.website_type} for website in agent.websites]
    return websites

@query_router.get("/agent/{agent_id}/listings")
async def get_agent_listings(agent_id: str, session: AsyncSession = Depends(get_session)):
    """
    Get a list of all listing IDs associated with an agent.
    """
    result = await session.execute(
        select(AgentModel).options(joinedload(AgentModel.listings)).where(AgentModel.encodedzuid == agent_id)
    )
    agent = result.unique().scalar_one_or_none()
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    listings = [listing.zpid for listing in agent.listings]
    return listings


@query_router.get("/listing/{listing_id}")
async def get_listing(listing_id: int, session: AsyncSession = Depends(get_session)):
    """
    Get listing information by listing ID.
    """
    result = await session.execute(select(ListingModel).where(ListingModel.zpid == listing_id))
    listing = result.unique().scalar_one_or_none()
    if listing is None:
        raise HTTPException(status_code=404, detail="Listing not found")
    return listing

