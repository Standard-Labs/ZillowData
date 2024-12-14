"""Fixtures, etc."""
import logfire
import pytest
from fastapi.testclient import TestClient
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from database.async_inserter import AsyncInserter
from api.query_endpoints import query_router
from fastapi import FastAPI
from typing import AsyncGenerator
from keys import KEYS
from scraper.models import Address, Agent, Listing, Website, Phones, Website

DATABASE_URL = f"postgresql+asyncpg://{KEYS.asyncpgCredentials.user}:{KEYS.asyncpgCredentials.password}@{KEYS.asyncpgCredentials.host}:{KEYS.asyncpgCredentials.port}/{KEYS.asyncpgCredentials.database}"

logfire.configure(
    token=KEYS.Logfire.write_token,
    environment=KEYS.Logfire.environment,
    scrubbing=False,
    send_to_logfire=True  
)


app = FastAPI(
    title="Zillow Database Scraper API",
    description=(
        "API for scraping Zillow and querying the database."
    )
)
logfire.instrument_fastapi(app, capture_headers=True)


app = FastAPI()
app.include_router(query_router)

@pytest.fixture(scope="session")
def async_inserter() -> AsyncInserter:
    return AsyncInserter(DATABASE_URL)

@pytest_asyncio.fixture(scope="function")
async def db_session(async_inserter: AsyncInserter) -> AsyncGenerator[AsyncSession, None]:
    async with async_inserter.get_session() as session:
        yield session

@pytest_asyncio.fixture(scope="function")
async def client() -> AsyncGenerator[AsyncSession, None]:
    with TestClient(app) as c:
        yield c

@pytest.fixture(scope="function")
def get_agent_model() -> Agent:
    return Agent(
    encodedZuid="agent1",
    business_name="Business One",
    fullName="Agent One",
    location="Location One",
    profileLink="http://profilelink1.com",
    email="agent1@example.com",
    is_team_lead=True,
    is_top_agent=True,
    sale_count_all_time=150,
    sale_count_last_year=20,
    sale_price_three_year_min=200000,
    sale_price_three_year_max=800000,
    ranking=1,
    page=1,
    specialties=["Residential"],
    phoneNumber="123-456-7890",
    phoneNumbers=Phones(
        cell="123-456-7890",
        brokerage="098-765-4321",
        business="111-222-3333"
    ),
    websites=[
        Website(url="http://website1.com", text="Personal"),
        Website(url="http://website2.com", text="Business")
    ],
    pastSales=[
        Listing(
            zpid=1001,
            type="Sale",
            bedrooms=3,
            bathrooms=2,
            latitude=40.7128,
            longitude=-74.0060,
            price=500000,
            price_currency="USD",
            status="Sold",
            home_type="Single Family",
            brokerage_name="Brokerage One",
            listing_url="http://listing1.com",
            represented="buyer",
            sold_date="2022-01-01",
            home_details_url="http://details1.com",
            living_area_value=1500,
            address=Address(
                line1="123 Main St",
                line2=None,
                city="New York",
                stateOrProvince="NY",
                postalCode="10001"
            )
        ),
        Listing(
            zpid=1002,
            type="Sale",
            bedrooms=4,
            bathrooms=3,
            latitude=34.0522,
            longitude=-118.2437,
            price=750000,
            price_currency="USD",
            status="Sold",
            home_type="Condo",
            brokerage_name="Brokerage Two",
            listing_url="http://listing2.com",
            represented="seller",
            sold_date="2022-02-01",
            home_details_url="http://details2.com",
            living_area_value=2000,
            living_area_units_short="sqft",
            mls_logo_src="http://mls2.com",
            address=Address(
                line1="456 Another St",
                line2="Apt 2",
                city="Los Angeles",
                stateOrProvince="CA",
                postalCode="90001"
            )
        )
    ],
    forRentListing=[],
    forSaleListing=[]
)


@pytest.fixture(scope="function")
def get_agent_model2() -> Agent:
    return Agent(
    encodedZuid="agent2",
    business_name="Business Two",
    fullName="Agent Two",
    location="Location Two",
    profileLink="http://profilelink2.com",
    email="agent2@example.com",
    is_team_lead=False,
    is_top_agent=False,
    sale_count_all_time=100,
    sale_count_last_year=15,
    sale_price_three_year_min=150000,
    sale_price_three_year_max=600000,
    ranking=2,
    page=2,
    specialties=["Luxury Homes"],
    phoneNumber="234-567-8901",
    phoneNumbers=Phones(
        cell="234-567-8901",
        brokerage="987-654-3210",
        business="222-333-4444"
    ),
    websites=[
        Website(url="http://website3.com", text="Personal"),
        Website(url="http://website4.com", text="Business")
    ],
    pastSales=[
        Listing(
            zpid=2001,
            type="Sale",
            bedrooms=5,
            bathrooms=4,
            latitude=37.7749,
            longitude=-122.4194,
            price=1000000,
            price_currency="USD",
            status="Sold",
            home_type="Townhouse",
            brokerage_name="Brokerage Three",
            has_open_house=False,
            represented="buyer",
            sold_date="2022-03-01",
            living_area_value=2500,
            living_area_units_short="sqft",
            mls_logo_src="http://mls3.com",
            address=Address(
                line1="789 Market St",
                line2=None,
                city="San Francisco",
                stateOrProvince="CA",
                postalCode="94103"
            )
        ),
        Listing(
            zpid=2002,
            type="Sale",
            bedrooms=2,
            bathrooms=1,
            latitude=41.8781,
            longitude=-87.6298,
            price=300000,
            price_currency="USD",
            status="Sold",
            home_type="Apartment",
            brokerage_name="Brokerage Four",
            listing_url="http://listing4.com",
            has_open_house=True,
            represented="seller",
            sold_date="2022-04-01",
            home_details_url="http://details4.com",
            living_area_value=1000,
            living_area_units_short="sqft",
            mls_logo_src="http://mls4.com",
            address=Address(
                line1="101 State St",
                line2="Unit 5",
                city="Chicago",
                stateOrProvince="IL",
                postalCode="60601"
            )
        )
    ],
    forRentListing=[],
    forSaleListing=[]
)