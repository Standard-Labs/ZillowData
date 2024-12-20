import json
import math
import time
from functools import wraps
import logfire
from pydantic import ValidationError
from typing import List
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import asyncio
import csv

from sqlalchemy import select
from database.async_inserter import AsyncInserter
from scraper.models import Website, Phones, Address, Listing, Agent, agent_types_default
from database.models import Agent as AgentModel, City as CityModel, Listing as ListingModel, AgentCity as AgentCityModel
from config import CONFIG
from keys import KEYS

API_KEY = KEYS.ScraperAPI.api_key
MAX_WORKERS = CONFIG.ScrapeWorkers.max_workers

def retry(retries=3, delay=2, return_value=None):
    """Retry decorator"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logfire.error(f"Error in {func.__name__} {args} {e}")
                    if attempt < retries:
                        # print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logfire.error(f"All {retries} attempts failed for {func.__name__} {args} {e}")
            return return_value

        return wrapper

    return decorator


def fetch_agent_data(url: str, payload: dict) -> str:
    """Fetch agent data using ScraperAPI"""
    response = requests.get('https://api.scraperapi.com/', params=payload)
    return response.text


def parse_json_data(script_tag) -> dict:
    """Parse JSON data from script tag"""
    json_data = script_tag.string
    return json.loads(json_data)


def extract_agents(parsed_data: dict, agent_type: str, page_number: int) -> List[Agent]:
    """Extract (initial) agent data from parsed data"""
    results = parsed_data['props']['pageProps']['proResults']['results']['professionals']
    agents = []
    for rank, agent_data in enumerate(results, start=1):
        agent = Agent(**agent_data)
        agent.specialties = [agent_type]
        agent.ranking = rank
        agent.page = page_number
        agents.append(agent)
    return agents


def remove_duplicates(agents: List[Agent]) -> List[Agent]:
    """Remove duplicate agents based on unique encodedzuid"""
    seen = set()
    unique_agents = []
    for agent in agents:
        if agent.encodedzuid not in seen:
            unique_agents.append(agent)
            seen.add(agent.encodedzuid)
    return unique_agents


@retry(retries=3, delay=5, return_value=1)
def get_max_pages(city_name, state, agent_type) -> int:
    """Get max pages for specified agent type"""

    url = f'https://www.zillow.com/professionals/real-estate-agent-reviews/{city_name}-{state.lower()}/?specialties={agent_type}&page=1'
    payload = {'api_key': API_KEY, 'url': url}

    response_text = fetch_agent_data(url, payload)
    soup = BeautifulSoup(response_text, 'html.parser')
    script_tag = soup.find("script", id="__NEXT_DATA__")

    if script_tag:
        parsed_data = parse_json_data(script_tag)
        total_agents = parsed_data['props']['pageProps']['proResults']['results']['total']
        max_pages = math.ceil(total_agents / 15)
        return 25 if max_pages > 25 else max_pages

    else:
        raise ValueError(f"(Max Pages) Script tag not found for {agent_type}")


@retry(retries=3, delay=2)
def handle_individual(agent: Agent) -> Agent:
    """Extract additional data for individual agent from their profile link"""
    if agent.profile_link:
        url = f'https://www.zillow.com/{agent.profile_link}'
        payload = {'api_key': API_KEY, 'url': url}
        response_text = fetch_agent_data(url, payload)
        soup = BeautifulSoup(response_text, 'html.parser')
        script_tag = soup.find("script", id="__NEXT_DATA__")

        if not script_tag:
            raise ValueError(f"(Individual) Script tag not found for {agent.full_name}")

        parsed_data = parse_json_data(script_tag)

        try:
            phones = parsed_data['props']['pageProps']['displayUser'].get('phoneNumbers', {})
            agent.phoneNumbers = Phones(**phones) if phones else None
        except Exception as e:
            # print(f"Error extracting phone numbers for {agent.full_name}: {e}")
            pass

        try:
            agent.email = parsed_data['props']['pageProps']['displayUser'].get('email', None)
        except Exception as e:
            # print(f"Error extracting email for {agent.full_name}: {e}")
            pass

        # Handle for-sale listings
        for_sale_listing = parsed_data['props']['pageProps'].get('forSaleListings', {})
        listings = for_sale_listing.get("listings", [])
        all_listing = []
        if listings:
            for listing in listings:
                try:
                    curr_list = Listing(**listing)
                    curr_list.type = "FOR SALE"
                    all_listing.append(curr_list)
                except ValidationError as e:
                    # print(f"Error validating FOR SALE listing for {agent.full_name}: {e}")
                    pass
                except Exception as e:
                    # print(f"Error processing FOR SALE listing for {agent.full_name}: {e}")
                    pass
        agent.forSaleListing = all_listing

        # Handle for-rent listings
        for_rent_listing = parsed_data['props']['pageProps'].get('forRentListings', {})
        rent_listings = for_rent_listing.get("listings", [])
        all_rent_listing = []
        if rent_listings:
            for listing in rent_listings:
                try:
                    curr_list = Listing(**listing)
                    curr_list.type = "FOR RENT"
                    all_rent_listing.append(curr_list)
                except ValidationError as e:
                    # print(f"Error validating FOR RENT listing for {agent.full_name}: {e}")
                    pass
                except Exception as e:
                    # print(f"Error processing FOR RENT listing for {agent.full_name}: {e}")
                    pass
        agent.forRentListing = all_rent_listing

        # Handle past sales
        past_sales = parsed_data['props']['pageProps'].get('pastSales', {})
        past_sale_infos = past_sales.get("past_sales", [])
        past_sale_listings = []
        if past_sale_infos:
            for past_sale_info in past_sale_infos:
                try:
                    listing = Listing(**past_sale_info)
                    listing.type = "PAST SALE"
                    listing.address = Address()
                    listing.address.line1 = past_sale_info.get("street_address", None)
                    listing.address.city = past_sale_info.get("city", None)
                    listing.address.state_or_province = past_sale_info.get("state", None)
                    zip = past_sale_info.get("city_state_zipcode", None)
                    listing.address.postal_code = past_sale_info.get("city_state_zipcode", None).split(", ")[2]
                    past_sale_listings.append(listing)
                except ValidationError as e:
                    # print(f"Error validating past sale for {agent.full_name}: {e}")
                    pass
                except Exception as e:
                    # print(f"Error processing past sale for {agent.full_name}: {e}")
                    pass
        agent.pastSales = past_sale_listings

        # Handle websites
        websites_data = parsed_data['props']['pageProps'].get('professionalInformation', [])
        websites_list = []
        for info in websites_data:
            if info.get("term") == "Websites":
                links = info.get("links", [])
                for link in links:
                    try:
                        websites_list.append(Website(**link))
                    except ValidationError as e:
                        # print(f"Error validating website for {agent.full_name}: {e}")
                        pass
                    except Exception as e:
                        # print(f"Error processing website for {agent.full_name}: {e}")
                        pass
        agent.websites = websites_list

        return agent
    else:
        # print(f"No profile link for {agent.full_name}")
        logfire.error(f"No profile link for {agent.full_name}")
        return agent


@retry(retries=3, delay=2)
def handle_page(city_name, state, agent_type, page_number) -> List[Agent]:
    """Initial scrape for agents on a page"""

    logfire.info(f"Initial Scrape for Page {page_number}  Agent Type: {agent_type}")
    url = f'https://www.zillow.com/professionals/real-estate-agent-reviews/{city_name}-{state.lower()}/?specialties={agent_type}&page={page_number}'
    payload = {'api_key': API_KEY, 'url': url}
    response_text = fetch_agent_data(url, payload)
    soup = BeautifulSoup(response_text, 'html.parser')
    script_tag = soup.find("script", id="__NEXT_DATA__")

    if script_tag:
        parsed_data = parse_json_data(script_tag)
        agents = extract_agents(parsed_data, agent_type, page_number)
        return agents
    else:
        raise ValueError(f"(Page) Script tag not found for {city_name} (Page {page_number}) Agent Type: {agent_type}")


def write_agents_to_csv(agents: List[Agent], file_name: str):
    """
    Write agent data to CSV
    Additional phone numbers ie(phoneNumbers data field in Agent) is seperated into
    multiple fields: cell, brokerage, business
    """

    if not agents:
        print("No agents to write.")
        return

    # Remove the "phoneNumbers" (not "phoneNumber") from the base headers,
    # and add "cell", "business", "brokerage" as separate columns
    # to represent the phoneNumbers data fields
    base_headers = list(agents[0].model_dump().keys())
    headers = [
                  header for header in base_headers
                  if header not in {"phoneNumbers"}
              ] + ["cell", "business", "brokerage"]

    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()

        for agent in agents:
            agent_data = agent.model_dump()

            # Exclude phoneNumbers data field from the row
            row = {key: value for key, value in agent_data.items() if key not in {"phoneNumbers"}}

            # Add "cell", "business", "brokerage" as separate columns to represent the phoneNumbers' data field
            phones = agent_data.get("phoneNumbers", {})
            row["cell"] = phones.get("cell", None)
            row["business"] = phones.get("business", None)
            row["brokerage"] = phones.get("brokerage", None)

            # Writing only the urls for the websites as a comma seperated list
            row["websites"] = [str(website.url) for website in agent.websites]

            writer.writerow(row)


def scrape(city, state, async_inserter: AsyncInserter, page_start: int | None = None, page_end: int | None = None, agent_types: list[str] | None = None) -> List[Agent]:
    """Main function to scrape data for specified city and state"""

    logfire.info(f"Scraping data for {city}, {state} for pages {page_start} to {page_end} and agent types: {agent_types}")

    asyncio.run(insert_status(city, state, "PENDING", async_inserter))

    try:
        agent_types = agent_types if agent_types is not None else agent_types_default

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as page_executor:
            futures = []
            agent_data = []
            for agent_type in agent_types:
                start = page_start if page_start is not None else 1
                agent_type_max_pages = get_max_pages(city, state, agent_type)
                end = page_end if page_end is not None else agent_type_max_pages

                for page_number in range(start, end + 1):
                    future = page_executor.submit(handle_page, city, state, agent_type, page_number)
                    futures.append(future)

            concurrent.futures.wait(futures)
            for future in futures:
                result = future.result()
                if result:
                    agent_data.extend(result)

        agent_data = remove_duplicates(agent_data)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as agent_executor:
            processed_agents = []
            for agent in agent_executor.map(handle_individual, agent_data):
                processed_agents.append(agent)

        asyncio.run(insert_status(city, state, "COMPLETED", async_inserter))
        return processed_agents

    except Exception as e:
        logfire.error(f"Error scraping data for {city}, {state}: {str(e)}")
        asyncio.run(insert_status(city, state, "ERROR", async_inserter))
        return []


def update_listing_data(city, state, async_inserter: AsyncInserter, agents: List[Agent]):
    """Update listing data for agents"""

    logfire.info(f"Updating ONLY listing data for {city}, {state}")
    asyncio.run(insert_status(city, state, "PENDING", async_inserter))

    try:
        async def get_profile_links(agent_ids: List[str]) -> List[Agent]:
            async with async_inserter.get_session() as session:
                result = await session.execute(
                    select(AgentModel.encodedzuid, AgentModel.profile_link).where(AgentModel.encodedzuid.in_(agent_ids))
                )
                return result.fetchall()

        profile_links = asyncio.run(get_profile_links(agents))

        all_agents = []
        for agentID, profile_link in profile_links:
            all_agents.append(Agent(encodedZuid=agentID, profileLink=profile_link))

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as listing_executor:
            futures = []
            for agent in all_agents:
                future = listing_executor.submit(handle_individual, agent)
                futures.append(future)

            concurrent.futures.wait(futures)
            updated_agents = [future.result() for future in futures]

        return updated_agents

    except Exception as e:
        logfire.error(f"Error updating listing data for {city}, {state}: {e}")
        asyncio.run(insert_status(city, state, "ERROR", async_inserter))
        return []    
    

async def insert_status(city: str, state: str, status: str, async_inserter: AsyncInserter):
    async with async_inserter.get_session() as session:
        await async_inserter.insert_status(city, state, status, session)

        