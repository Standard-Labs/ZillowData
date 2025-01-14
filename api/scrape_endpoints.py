""" This module contains the FastAPI endpoints for (re)/initializing jobs and status. """
import asyncio
from typing import Dict
from fastapi import APIRouter, Response
import logfire
from pydantic import BaseModel
from starlette import status
from scraper.scrape import scrape, update_listing_data, update_initial_data
from scraper.models import JobStatus, ScrapeJobPayload
from database.models import Agent as AgentModel, City as CityModel, AgentCity as AgentCityModel
from sqlalchemy.future import select
from database.async_inserter import AsyncInserter
from api.async_inserter import async_inserter as asyncInserter

scrape_router = APIRouter()
scrape_lock = asyncio.Lock()

def get_async_inserter() -> AsyncInserter:
    return asyncInserter

class InitialDataRequest(BaseModel):
    data: Dict[str, str]


@scrape_router.post("/scrape")
async def handle_job(payload: ScrapeJobPayload, response: Response = None):
    """

    *******************************************************************************************************
    *** NOTE: Temporary Usage: Set 'update_existing' to True for ALL Requests Here Until Further Notice ***
    *** This is because, we're doing the scraping in parts now, and not all in one request ***
    *******************************************************************************************************

    if rescrape is True, rescrape the data for the city and state, regardless of the current status.
    if rescrape is False, initialize a new job ONLY IF the data does not exist OR the job encountered an error previously (JobStatus.NOT_SCRAPED or JobStatus.ERROR)

    ***Only set updateExisting to True if ALL agent data needs to be updated, mainly because core agent data will likely not change, only listings, so use the other endpoint '/update/listings' for that***

    200: Job Was Completed Successfully (Either New Job Or Re-scrape)
    422: Data Already Exists For City, State. Set 'rescrape' Flag To True To Update Data.
    409: Job is in progress
    500: Error in New Job or Re-scrape

    """
    try:
        city = payload.city.upper()
        state = payload.state.upper()

        async with asyncInserter.get_session() as session:
            job_status = await asyncInserter.check_status(city, state, session)

        if payload.update_existing:
            await scrape_and_insert(payload)
            async with asyncInserter.get_session() as session:
                complete_status = await asyncInserter.check_status(city, state, session)

            if complete_status is JobStatus.COMPLETED:
                response.status_code = status.HTTP_200_OK
                return {"message": f"Job completed successfully for {city}, {state}"}
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                return {"message": "Error", "error": f"Error in Re-scraping/insertion job. {complete_status}"}

        else:
            if job_status is JobStatus.NOT_SCRAPED or job_status is JobStatus.ERROR:
                logfire.info(f"Initializing job for {city}, {state}")
                await scrape_and_insert(payload)
                async with asyncInserter.get_session() as session:
                    complete_status = await asyncInserter.check_status(city, state, session)

                if complete_status is JobStatus.COMPLETED:
                    response.status_code = status.HTTP_200_OK
                    return {"message": f"Job completed successfully for {city}, {state}"}
                else:
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return {"message": "Error", "error": f"Error in scraping/insertion job. {complete_status}"}

            elif job_status is JobStatus.COMPLETED:
                response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
                return {"message": f"Data is Already Available For {city}, {state}. Set 'update_existing' Flag To True To Update Data."}

            elif job_status is JobStatus.PENDING:
                response.status_code = status.HTTP_409_CONFLICT
                return {"message": job_status.message(city, state)}

            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                return {"message": "Error", "error": job_status.message(city, state)}

    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "Error", "error": str(e)}    


@scrape_router.post("/update/listings")
async def update_listings(payload: ScrapeJobPayload, response: Response = None):
    """
    Update listings for a city and state. This will only update the listings for the city and state, not the core agent data.
    200: Job Was Completed Successfully
    409: Job is in progress
    404: Job Has Not Been Initialized
    422: Job Encountered An Error
    500: Error in New Job or Re-scrape
    """
    try:
        city = payload.city.upper()
        state = payload.state.upper()

        async with asyncInserter.get_session() as session:
            job_status = await asyncInserter.check_status(city, state, session)

        if job_status is JobStatus.COMPLETED:
            result = await session.execute(
                select(AgentModel.encodedzuid).join(AgentCityModel).join(CityModel).where(
                CityModel.city == city.upper(),
                CityModel.state == state.upper()
                )
            )
            agent_ids = [row[0] for row in result.fetchall()]
            if not agent_ids:
                return {"message": "No agents found for the specified city and state"}
            
            await scrape_and_insert(payload, update_listings=True, agent_ids=agent_ids)
            
            async with asyncInserter.get_session() as session:
                complete_status = await asyncInserter.check_status(city, state, session)

            if complete_status is JobStatus.COMPLETED:
                response.status_code = status.HTTP_200_OK
                return {"message": f"Job completed successfully for {city}, {state}"}
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                return {"message": "Error", "error": f"Error in Re-scraping/insertion job. {complete_status}"}

        elif job_status is JobStatus.NOT_SCRAPED:
            response.status_code = status.HTTP_404_NOT_FOUND
            return {"message": job_status.message(city, state) + "Cannot update listings with any pre-existing data."}

        elif job_status is JobStatus.PENDING:
            response.status_code = status.HTTP_409_CONFLICT
            return {"message": job_status.message(city, state) + "Cannot update listings while job is in progress."}

        elif job_status is JobStatus.ERROR:
            response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
            return {"message": "Error", "error": job_status.message(city, state) + "Cannot update listings, original job previously an error..."}

        else:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return {"message": "Error", "error": job_status.message(city, state) + "Cannot update listings, unknown error in getting status..."}

    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "Error", "error": str(e)}


@scrape_router.get("/status/{city}/{state}")
async def check_status(city: str, state: str, response: Response = None):
    """
    Check the status of the scraping job for a city and state.
    200: Job Was Completed Successfully
    409: Job is in progress
    404: Job Has Not Been Initialized
    422: Job Encountered An Error
    500: Error checking status
    """
    try:
        city = city.upper()
        state = state.upper()
        
        async with asyncInserter.get_session() as session:
            job_status = await asyncInserter.check_status(city, state, session)

        if job_status is JobStatus.COMPLETED:
            response.status_code = status.HTTP_200_OK
            return {"message": job_status.message(city, state)}
        elif job_status is JobStatus.NOT_SCRAPED:
            response.status_code = status.HTTP_404_NOT_FOUND
            return {"message": job_status.message(city, state)}
        elif job_status is JobStatus.PENDING:
            response.status_code = status.HTTP_409_CONFLICT
            return {"message": job_status.message(city, state)}
        elif job_status is JobStatus.ERROR:
            response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
            return {"message": "Error", "error": job_status.message(city, state)} 
        else:
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return {"message": "Error", "error": job_status.message(city, state)}
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"message": "Error", "error": str(e)}


async def scrape_and_insert(payload: ScrapeJobPayload, update_listings=False, agent_ids=None):
    """
    update_listings, just a flag so we only update listings, not core agent data
    if payload.update_existing is True, everything will be updated(including listings)
    """

    try:
        logfire.info(f"Awaiting scrape lock for {payload.city}, {payload.state}...")
        await scrape_lock.acquire()
        if update_listings:
            logfire.info(f"Scrape lock acquired for {payload.city}, {payload.state}... Starting scrape for listings only")
        else:
            logfire.info(f"Scrape lock acquired for {payload.city}, {payload.state}... Starting scrape")
        
        try:
            if update_listings:
                agents = await asyncio.to_thread(update_listing_data, payload.city, payload.state, asyncInserter, agent_ids)
            else:
                agents = await asyncio.to_thread(scrape, payload.city, payload.state, asyncInserter, payload.page_start, payload.page_end, payload.agent_types)
        finally:
            scrape_lock.release()

        if not update_listings:
            await asyncInserter.insert_agents(agents, payload.city, payload.state, payload.update_existing)
        else:
            await asyncInserter.insert_updated_listings(agents, payload.city, payload.state)

    except Exception as e:
        logfire.error(f"Error in scrape_and_insert for {payload.city}, {payload.state}. Error: {str(e)}")


@scrape_router.post("/update-initial-data")
async def update_initial_data_route(payload: InitialDataRequest, response: Response = None):
    """ 
    Pass in data as {"encodedzuid": "profilelink"} for each agent to update the initial data for a city and state 
    This is for those cities where only initial data was scraped, and we need to now get the specific profile data for each agent
    
    Temporary fix to split up scraping process for profile data
    run the run/update_initial.py script to hit this endpoint
    """
    
    try:
        logfire.info(f"Awaiting scrape lock to update initial data for {payload.data}...")
        await scrape_lock.acquire()
        logfire.info(f"Scrape lock acquired for {payload.data}... Starting scrape to update initial data")
        
        try:
            agents = await asyncio.to_thread(update_initial_data, payload.data)
        finally:
            scrape_lock.release()

        await asyncInserter.db_update_initial_data(agents)

    except Exception as e:
        logfire.error(f"Error in update initial data for {payload.data}. Error: {str(e)}")