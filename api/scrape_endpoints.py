""" This module contains the FastAPI endpoints for (re)/initializing jobs and status. """
from typing import Optional
from fastapi import APIRouter
from starlette import status
from database.inserter import Inserter
from scraper.scrape import scrape
from api.db_init import supabase_client

scrape_router = APIRouter()


# Some notes
# 1. The scrape_and_insert function is a temporary structure for now. It can be improved later.
# 2. Future improvements to possibly either make scraping and/or insertion asynchronous if the need even arises.
# 3. OR, more simply implement task queue like Celery to offload the scraping and insertion tasks, wouldn't make it
# faster. but would make this module cleaner, and make dealing with this endpoint easier.
# 4. This whole module is a mess lol 


# Note this endpoint must be asynchronous.
# Even though scraping and insertion are not asynchronous, the endpoint should be asynchronous, so concurrent requests
# are handled SEQUENTIALLY. This is to prevent multiple scrapes from happening at the same time, which leads to
# exceeding max threads allowed by Scraper API.
@scrape_router.get("/scrape/{city}/{state}")
async def initialize_job(city: str, state: str, max_pages: int | None = None):
    """
        scrape/city/state?max_pages=1 or scrape/city/state

        Attempts to initialize a scraping/insertion job for a city and state.

        Returns 200 if a job is completed successfully
        Returns 409 if a job is in progress
        Returns 500 if a job cannot be initialized or completed successfully along with a specific error message
    """
    try:
        city = city.upper()
        state = state.upper()
        job_status = supabase_client.check_status(city, state)

        if job_status.get('status') == 200:
            return ({"message": f"Data is Already Available For {city}, {state}. Use /rescrape to update data."},
                    status.HTTP_200_OK)

        elif job_status.get('status') == 409:
            return (
                {"message": f"Scraping/Insertion is currently in progress for {city}, {state}. Use /status to check "
                            f"status."}, status.HTTP_409_CONFLICT)

        elif job_status.get('status') == 422 or job_status.get('status') == 404:
            # A previously failed job or a job that has not been initialized yet
            scrape_and_insert(city, state, max_pages)
            complete_status = supabase_client.check_status(city, state)

            if complete_status.get('status') == 200:
                return {"message": complete_status.get('message')}, status.HTTP_200_OK
            else:
                return ({"message": "Error", "error": f"Error in scraping/insertion job for {city}, {state}, "
                                                      f"{complete_status.get('message')}"},
                        status.HTTP_500_INTERNAL_SERVER_ERROR)

        else:
            return ({"message": "Error", "error": f"Error Checking Initial Job Status For {city}, {state}"},
                    status.HTTP_500_INTERNAL_SERVER_ERROR)

    except Exception as e:
        return {"message": "Error", "error": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


@scrape_router.get("/rescrape/{city}/{state}")
async def rescrape_data(city: str, state: str, max_pages: None):
    """
    Attempts to re-scrape and update data for a city and state.

    Returns 200 if a job is completed successfully
    Returns 409 if a job is in progress
    Returns 404 if there is no initial data to update (use /scrape to initialize a new scrape job)
    Returns 500 if a job cannot be re-initialized or completed successfully along with a specific error message

    """

    try:
        city = city.upper()
        state = state.upper()
        job_status = supabase_client.check_status(city, state)


        if job_status.get('status') == 404:
            return ({"message": f"Cannot Update Data For {city}, {state}. There Is No Initial Data. Use /scrape to "
                                f"initialize a new scrape job"}, status.HTTP_404_NOT_FOUND)

        elif job_status.get('status') == 409:
            return (
                {"message": f"Scraping/Insertion is currently in progress for {city}, {state}. Use /status to check "
                            f"status."}, status.HTTP_409_CONFLICT)

        elif job_status.get('status') == 422 or job_status.get('status') == 200:
            # A previously failed job or a job that has been prev. been completed(data is available)
            scrape_and_insert(city, state, max_pages)
            complete_status = supabase_client.check_status(city, state)

            if complete_status.get('status') == 200:
                return {"message": f'{complete_status.get('message')} + (Re-scraped)'}, status.HTTP_200_OK
            else:
                return ({"message": "Error", "error": f"Error in Re-scraping/insertion job for {city}, {state}, "
                                                      f"{complete_status.get('message')}"},
                        status.HTTP_500_INTERNAL_SERVER_ERROR)

        else:
            return ({"message": "Error", "error": f"Error Checking Initial Job Status For {city}, {state}"},
                    status.HTTP_500_INTERNAL_SERVER_ERROR)

    except Exception as e:
        return {"message": "Error", "error": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


@scrape_router.get("/status/{city}/{state}")
def check_status(city: str, state: str):
    """
    Check the status of the scraping job for a city and state.
    200: Scraping/Data Insertion has not yet been initialized
    409: Scraping is in progress
    204: Scraping is completed
    499: Scraping previously failed
    500: Error checking status
    """
    try:
        city = city.upper()
        state = state.upper()
        job_status = supabase_client.check_status(city, state)
        resp = job_status.get('status')

        if resp == 200:
            return ({"message": f"Scraping/Data Insertion Was Successful For {city}, {state}"},
                    status.HTTP_200_OK)
        elif resp == 404:
            return ({"message": f"Scraping/Date Insertion Has NOT Been Started For {city}, {state}"},
                    status.HTTP_409_CONFLICT)
        elif resp == 409:
            return {"message": f"Scraping/Data Insertion is PENDING for {city}, {state}"}, status.HTTP_204_NO_CONTENT
        elif resp == 422:
            return ({"message": "Error", "error": f"Scraping Failed for {city}, {state}. Please initialize a new scrape"
                                                  f" job."}, status.HTTP_499_CLIENT_CLOSED_REQUEST)
        else:
            return ({"message": "Error", "error": f"Error checking status for {city}, {state}"},
                    status.HTTP_500_INTERNAL_SERVER_ERROR)
    except Exception as e:
        return {"message": "Error", "error": str(e)}, status.HTTP_500_INTERNAL_SERVER_ERROR


def scrape_and_insert(city: str, state: str, max_pages: None):
    """
        Job Orchestrator: Scrapes data for a city and state, and inserts it into the database.
        Temporary structure for now, can improve later.
        **Does not return anything**
        **Any Errors Are Handled Within Scraper and Inserter, and the status is updated accordingly.**

    """
    try:
        inserter = Inserter(supabase_client)
        agents = scrape(city, state, supabase_client, max_pages)
        inserter.insert_agents(agents, city, state)

    except Exception as e:
        print(f"Error scraping and inserting data for {city}, {state}: {e}")

