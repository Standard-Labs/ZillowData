""" This module contains the FastAPI endpoints for (re)/initializing jobs and status. """
from fastapi import APIRouter, Response
import logfire
from starlette import status
from database.inserter import Inserter
from scraper.scrape import scrape
from scraper.models import JobStatus
from api.db_init import supabase_client

scrape_router = APIRouter()


# Some notes
# 1. The scrape_and_insert function is a temporary structure for now. It can be improved later.
# 2. Future improvements to possibly either make scraping and/or insertion asynchronous if the need even arises.
# 3. OR, more simply implement task queue like Celery to offload the scraping and insertion tasks, wouldn't make it
# faster. but would make this module cleaner, and make dealing with this endpoint easier.
# 4. This whole module is a mess lol 


@scrape_router.get("/scrape/{city}/{state}")
async def handle_job(city: str, state: str, max_pages: int | None = None, rescrape: bool | None = None, response: Response = None):
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

        if rescrape:
            scrape_and_insert(city, state, max_pages)
            complete_status = supabase_client.check_status(city, state)

            if complete_status is JobStatus.COMPLETED:
                response.status_code = status.HTTP_200_OK
                return {"message": {complete_status.message(city, state)}}
            else:
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                return {"message": "Error", "error": f"Error in Re-scraping/insertion job. {complete_status.message(city, state)}"}  

        else:
            job_status = supabase_client.check_status(city, state)

            if job_status is JobStatus.NOT_SCRAPED or job_status is JobStatus.ERROR:
                logfire.info(f"Initializing job for {city}, {state}")
                scrape_and_insert(city, state, max_pages)
                complete_status = supabase_client.check_status(city, state)

                if complete_status is JobStatus.COMPLETED:
                    response.status_code = status.HTTP_200_OK
                    return {"message": complete_status.message(city, state)}
                else:
                    response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                    return {"message": "Error", "error": complete_status.message(city, state)}

            elif job_status is JobStatus.COMPLETED:
                response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
                return {"message": f"Data is Already Available For {city}, {state}. Set 'rescrape' Flag To True To Update Data."}

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
        job_status = supabase_client.check_status(city, state)

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


def scrape_and_insert(city: str, state: str, max_pages: int | None = None):
    """
        Job Orchestrator: Scrapes data for a city and state, and inserts it into the database.
        Temporary structure for now, can improve later.
        **Does not return anything**
        **Any Errors Are Handled Within Scraper and Inserter, and the status is updated accordingly**
        Will handle requests sequentially

    """
    try:
        inserter = Inserter(supabase_client)
        agents = scrape(city, state, supabase_client, max_pages)
        inserter.insert_agents(agents, city, state)
        
        # Switch to async database client if needed in the future for perfomance
        # loop = asyncio.get_event_loop()
        # await loop.run_in_executor(None, inserter.insert_agents, agents, city, state)

    except Exception as e:
        logfire.error(f"Error in scrape_and_insert for {city}, {state}. Error: {str(e)}")

