# """"""
# import pytest
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.future import select
# from sqlalchemy import delete
# from database.models import Base, Agent as AgentModel, Listing as ListingModel
# from database.async_inserter import AsyncInserter
# from fastapi.testclient import TestClient
# from api.query_endpoints import query_router
# from fastapi import FastAPI

# TEST_DATABASE_URL = ""

# app = FastAPI()
# app.include_router(query_router)

# async_inserter = AsyncInserter(TEST_DATABASE_URL)

# TestSessionLocal = sessionmaker(
#     bind=create_async_engine(TEST_DATABASE_URL, echo=True),
#     class_=AsyncSession,
#     expire_on_commit=False
# )

# @pytest.fixture(scope="module")
# async def setup_database():
#     async with async_inserter.get_session() as session:
#         async with session.begin():
#             await session.run_sync(Base.metadata.create_all)
#     yield
#     # Drop the test database tables
#     async with async_inserter.get_session() as session:
#         async with session.begin():
#             await session.run_sync(Base.metadata.drop_all)

# @pytest.fixture(scope="function")
# async def db_session():
#     async with TestSessionLocal() as session:
#         yield session

# @pytest.fixture(scope="function")
# def client():
#     with TestClient(app) as c:
#         yield c

# @pytest.mark.asyncio
# async def test_insert_and_query_agent(db_session: AsyncSession, client: TestClient):
#     agent = AgentModel(
#         encodedzuid="test_agent",
#         business_name="Test Business",
#         full_name="Test Agent",
#         location="Test Location",
#         profile_link="http://testprofile.link",
#         email="test@example.com",
#         is_team_lead=True,
#         is_top_agent=True,
#         sale_count_all_time=100,
#         sale_count_last_year=10,
#         sale_price_range_three_year_min=100000,
#         sale_price_range_three_year_max=500000,
#         ranking=1,
#         page=1,
#         specialties=["specialty1", "specialty2"]
#     )
#     db_session.add(agent)
#     await db_session.commit()

#     response = client.get(f"/agent/{agent.encodedzuid}")
#     assert response.status_code == 200
#     assert response.json()["encodedzuid"] == agent.encodedzuid

#     await db_session.execute(delete(AgentModel).where(AgentModel.encodedzuid == agent.encodedzuid))
#     await db_session.commit()

# @pytest.mark.asyncio
# async def test_insert_and_query_listing(db_session: AsyncSession, client: TestClient):
#     listing = ListingModel(
#         zpid=123456,
#         type="Test Type",
#         bedrooms=3,
#         bathrooms=2,
#         latitude=40.7128,
#         longitude=-74.0060,
#         price="500000",
#         price_currency="USD",
#         status="For Sale",
#         home_type="Single Family",
#         brokerage_name="Test Brokerage",
#         home_marketing_status="Active",
#         home_marketing_type="Standard",
#         listing_url="http://testlisting.link",
#         has_open_house=True,
#         represented=True,
#         sold_date=None,
#         home_details_url="http://testdetails.link",
#         living_area_value=1500,
#         living_area_units_short="sqft",
#         mls_logo_src="http://testmls.logo",
#         line1="123 Test St",
#         line2="Apt 1",
#         state_or_province="NY",
#         city="New York",
#         postal_code="10001"
#     )
#     db_session.add(listing)
#     await db_session.commit()

#     response = client.get(f"/listing/{listing.zpid}")
#     assert response.status_code == 200
#     assert response.json()["zpid"] == listing.zpid

#     await db_session.execute(delete(ListingModel).where(ListingModel.zpid == listing.zpid))
#     await db_session.commit()