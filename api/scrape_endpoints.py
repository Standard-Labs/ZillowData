""" This module contains the FastAPI endpoints for (re)/initializing jobs and status. """
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, Response
import logfire
from starlette import status
from scraper.scrape import scrape
from scraper.models import JobStatus
from keys import KEYS
from database.async_inserter import AsyncInserter

scrape_router = APIRouter()
scrape_lock = asyncio.Lock()

DATABASE_URL =f"postgresql+asyncpg://{KEYS.asyncpgCredentials.user}:{KEYS.asyncpgCredentials.password}@{KEYS.asyncpgCredentials.host}:{KEYS.asyncpgCredentials.port}/{KEYS.asyncpgCredentials.database}"


@scrape_router.get("/scrape/{city}/{state}")
async def handle_job(city: str, state: str, max_pages: int | None = None, update_existing: bool | None = None, response: Response = None):
    """
    /scrape/{city}/{state}?max_pages={max_pages}&rescrape={rescrape}
    /scrape/{city}/{state}?rescrape={rescrape}
    /scrape/{city}/{state}?max_pages={max_pages}
    /scrape/{city}/{state}

    if rescrape is True, rescrape the data for the city and state, regardless of the current status.
    if rescrape is False, initialize a new job ONLY IF the data does not exist OR the job encountered an error previously (JobStatus.NOT_SCRAPED or JobStatus.ERROR)

    200: Job Was Completed Successfully (Either New Job Or Re-scrape)
    422: Data Already Exists For City, State. Set 'rescrape' Flag To True To Update Data.
    409: Job is in progress
    500: Error in New Job or Re-scrape

    """
    try:
        city = city.upper()
        state = state.upper()
        asyncInserter = AsyncInserter(DATABASE_URL)

        async with asyncInserter.get_session() as session:
            job_status = await asyncInserter.check_status(city, state, session)

        if update_existing:
            await scrape_and_insert(city, state, max_pages, update_existing)
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
                await scrape_and_insert(city, state, max_pages)
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
        
        asyncInserter = AsyncInserter(DATABASE_URL)
        async with asyncInserter.SessionLocal() as session:
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


async def scrape_and_insert(city: str, state: str, max_pages: int | None = None, update_existing: bool | None = None):
    try:
        asyncInserter = AsyncInserter(DATABASE_URL)

        await scrape_lock.acquire()  # one scrape job at a time, insertion can be async, parallel
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                agents = await loop.run_in_executor(pool, scrape, city, state, max_pages)
        finally:
            scrape_lock.release()

        await asyncInserter.insert_agents(agents, city, state)

    except Exception as e:
        logfire.error(f"Error in scrape_and_insert for {city}, {state}. Error: {str(e)}")

