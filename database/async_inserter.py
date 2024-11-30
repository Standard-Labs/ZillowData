from contextlib import asynccontextmanager
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import pool
from scraper.models import Agent, JobStatus
from database.models import City as CityModel, Status as StatusModel, Agent as AgentModel, Listing as ListingModel, ListingAgent as ListingAgentModel, AgentCity as AgentCityModel, Phone as PhoneModel, Website as WebsiteModel
import logfire
import uuid


class AsyncInserter:

    def __init__(self, db_url: str):
        logfire.info("Initializing AsyncInserter")
        self.engine = create_async_engine(
            db_url,
            poolclass=pool.NullPool,  # important setting #1 , but maybe it's not required actually
            future=True,
            connect_args={  # important settings for asyncpg
                "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
            },
        )

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        logfire.info("AsyncInserter initialized")

    @asynccontextmanager
    async def get_session(self):
        async with self.SessionLocal() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def check_status(self, city: str, state: str, session: AsyncSession) -> str:
        """
        Check the status of the scraping job for the specified city and state.
        """
        try:
            result = await session.execute(
                select(StatusModel.job_status).join(CityModel).where(CityModel.city == city.upper(), CityModel.state == state.upper())
            )
            job_status = result.scalar()
            
            if job_status == "COMPLETED":
                # Job has previously been completed successfully
                return JobStatus.COMPLETED

            elif job_status == "PENDING":
                # Job is still in progress
                return JobStatus.PENDING

            elif job_status == "ERROR":
                # Job encountered an error previously
                return JobStatus.ERROR

            elif job_status == "UNKNOWN":
                # Unknown status
                return JobStatus.UNKNOWN
            elif job_status is None:
                return JobStatus.NOT_SCRAPED
        except Exception as e:
            logfire.error(f"Error checking status for city: {city}, state: {state}: {e}")
            return "NOT_SCRAPED"

    async def insert_city(self, city: str, state: str, session: AsyncSession) -> int | None:
        """
        Insert the city into the City Table if it does not exist, else return the city.
        """
        try:
            city = city.upper()
            state = state.upper()

            result = await session.execute(
                select(CityModel.id).where(CityModel.city == city, CityModel.state == state)
            )
            city_id = result.scalar()

            if not city_id:
                city_data = {"city": city, "state": state}
                stmt = pg_insert(CityModel).values(city_data).on_conflict_do_nothing()
                await session.execute(stmt)
                await session.commit()

                result = await session.execute(
                    select(CityModel.id).where(CityModel.city == city, CityModel.state == state)
                )
                city_id = result.scalar()

            return city_id
        except Exception as e:
            logfire.error(f"Error inserting city {city}, {state}: {e}")
            await session.rollback()
            return None

    async def insert_status(self, city: str, state: str, status: str, session: AsyncSession) -> None:
        """
        Insert the status of the job for the specified city and state.
        Status can be one of: PENDING, COMPLETED, ERROR.
        """
        try:
            logfire.info(f"Inserting status for city: {city}, state: {state}, status: {status}")
            city_id = await self.insert_city(city, state, session)
            if city_id:
                status_data = {"city_id": city_id, "job_status": status, "last_updated": func.now()}
                stmt = pg_insert(StatusModel).values(status_data).on_conflict_do_update(
                    index_elements=["city_id"],
                    set_={"job_status": status, "last_updated": func.now()}
                )
                await session.execute(stmt)
                await session.commit()
                logfire.info(f"Status inserted for city ID: {city_id}")
        except Exception as e:
            await session.rollback()
            logfire.error(f"Error inserting status for {city}, {state}: {e}")

    def prepare_agent_city(self, agent: Agent, city_id: str) -> dict:
        """Prepare the agent-city junction table data."""
        logfire.info(f"Preparing agent-city data for agent: {agent.encodedzuid}, city ID: {city_id}")
        return {"agent_id": agent.encodedzuid, "city_id": city_id}

    async def prepare_individual_agent(self, agent: Agent, city: str, state: str, session: AsyncSession) -> dict | None:
        """
        Prepare the agent data to be inserted into the database.
        Returns the AgentModel object if the agent does not exist in the database, else returns None.
        If the agent exists, the agent data is updated (specialties are appended, page and ranking are updated).
        """
        try:
            logfire.info(f"Preparing individual agent data for agent: {agent.encodedzuid}")
            result = await session.execute(
                select(AgentModel.ranking, AgentModel.page, AgentModel.specialties).where(AgentModel.encodedzuid == agent.encodedzuid)
            )
            og_agent_data = result.fetchone()

            if og_agent_data:
                og_ranking, og_page, og_specialties = og_agent_data

                new_page = None
                new_ranking = None

                if agent.page is not None:
                    new_page = min(agent.page, og_page if og_page is not None else agent.page)
                    if new_page == og_page:
                        new_ranking = og_ranking if og_ranking is not None else agent.ranking
                    else:
                        new_ranking = agent.ranking
                else:
                    new_page = og_page
                    new_ranking = og_ranking

                updated_specialties = list(set(agent.specialties + og_specialties if og_specialties is not None else agent.specialties))

                agent_data = {
                    "encodedzuid": agent.encodedzuid,
                    "business_name": agent.business_name,
                    "full_name": agent.full_name,
                    "location": agent.location,
                    "profile_link": agent.profile_link,
                    "sale_count_all_time": agent.sale_count_all_time,
                    "sale_count_last_year": agent.sale_count_last_year,
                    "sale_price_range_three_year_min": agent.sale_price_three_year_min,
                    "sale_price_range_three_year_max": agent.sale_price_three_year_max,
                    "is_team_lead": agent.is_team_lead,
                    "is_top_agent": agent.is_top_agent,
                    "email": agent.email,
                    "ranking": new_ranking,
                    "page": new_page,
                    "specialties": updated_specialties
                }
                logfire.info(f"Prepared updated agent data for agent: {agent.encodedzuid}")
                return agent_data

            else:
                agent_data = {
                    "encodedzuid": agent.encodedzuid,
                    "business_name": agent.business_name,
                    "full_name": agent.full_name,
                    "location": agent.location,
                    "profile_link": agent.profile_link,
                    "sale_count_all_time": agent.sale_count_all_time,
                    "sale_count_last_year": agent.sale_count_last_year,
                    "sale_price_range_three_year_min": agent.sale_price_three_year_min,
                    "sale_price_range_three_year_max": agent.sale_price_three_year_max,
                    "is_team_lead": agent.is_team_lead,
                    "is_top_agent": agent.is_top_agent,
                    "email": agent.email,
                    "ranking": agent.ranking,
                    "page": agent.page,
                    "specialties": agent.specialties
                }
                logfire.info(f"Prepared new agent data for agent: {agent.encodedzuid}")
                return agent_data
        except Exception as e:
            logfire.error(f"Error preparing individual agent data for agent {agent.encodedzuid}: {e}")
            await session.rollback()
            return None

    async def prepare_phones(self, agent: Agent, agent_exists: bool, session: AsyncSession) -> List[dict]:
        try:
            logfire.info(f"Preparing phone data for agent: {agent.encodedzuid}")
            phone_data = {}
            if agent.phoneNumber:
                phone_data['primary'] = agent.phoneNumber
            if agent.phoneNumbers.cell:
                phone_data['cell'] = agent.phoneNumbers.cell
            if agent.phoneNumbers.brokerage:
                phone_data['brokerage'] = agent.phoneNumbers.brokerage
            if agent.phoneNumbers.business:
                phone_data['business'] = agent.phoneNumbers.business

            if agent_exists:
                await session.execute(
                    delete(PhoneModel).where(PhoneModel.agent_id == agent.encodedzuid)
                )

            # Prepare new phones data
            phones = []
            unique_phone_agent_pairs = set()

            for phone_type, phone in phone_data.items():
                if phone:
                    phone_agent_pair = (phone, agent.encodedzuid)
                    if phone_agent_pair not in unique_phone_agent_pairs:
                        unique_phone_agent_pairs.add(phone_agent_pair)
                        phones.append({
                            'phone': phone,
                            'agent_id': agent.encodedzuid,
                            'type': phone_type
                        })

            logfire.info(f"Prepared phone data for agent: {agent.encodedzuid}")
            return phones

        except Exception as e:
            logfire.error(f"Error preparing phone data for agent {agent.encodedzuid}: {e}")
            await session.rollback()
            return []

    async def prepare_websites(self, agent: Agent, agent_exists: bool, session: AsyncSession) -> List[dict]:
        try:
            logfire.info(f"Preparing website data for agent: {agent.encodedzuid}")
            unique_websites = set()
            websites = []

            if agent_exists:
                await session.execute(
                    delete(WebsiteModel).where(WebsiteModel.agent_id == agent.encodedzuid)
                )

            for website in agent.websites:
                if website.website_url in unique_websites:
                    continue
                unique_websites.add(website.website_url)

                websites.append({
                    'website_url': str(website.website_url),
                    'agent_id': agent.encodedzuid,
                    'website_type': website.website_type
                })

            logfire.info(f"Prepared website data for agent: {agent.encodedzuid}")
            return websites
        except Exception as e:
            logfire.error(f"Error preparing website data for agent {agent.encodedzuid}: {e}")
            await session.rollback()
            return []
        
    async def insert_listings(self, agent: Agent,  agent_exists: bool, session: AsyncSession):
        """
        Batch Insert the listings data for the agent.
        Also batch insert the junction table data for the agent-listing relationship.
        """
        try:
            logfire.info(f"Inserting listings for agent: {agent.encodedzuid}")
            listings_all_data = []
            listing_agent_data = []
            seen_zpids = set()

            if agent_exists:

                # deletes in junction table
                await session.execute(
                    delete(ListingAgentModel).where(ListingAgentModel.agent_id == agent.encodedzuid)
                )
                await session.commit()

                # deletes in listing table if the listing is not in the junction table for ANY agent
                # this is to prevent orphaned listings
                await session.execute(
                                delete(ListingModel).where(
                                    ListingModel.zpid.in_(
                                        select(ListingModel.zpid).where(
                                            ~ListingModel.zpid.in_(
                                                select(ListingAgentModel.listing_id)
                                            )
                                        )
                                    )
                                )
                            )
                
                await session.commit()


            for listing in agent.pastSales + agent.forRentListing + agent.forSaleListing:
                if listing.zpid in seen_zpids:
                    logfire.info(f"Duplicate listing found for zpid {listing.zpid}, skipping.")
                    continue
                seen_zpids.add(listing.zpid)

                listings_all_data.append({
                    "zpid": listing.zpid,
                    "type": listing.type or None,
                    "bedrooms": listing.bedrooms or None,
                    "bathrooms": listing.bathrooms or None,
                    "latitude": listing.latitude or None,
                    "longitude": listing.longitude or None,
                    "price": str(listing.price) or None,
                    "price_currency": listing.price_currency or None,
                    "status": listing.status or None,
                    "home_type": listing.home_type or None,
                    "brokerage_name": listing.brokerage_name or None,
                    "home_marketing_status": listing.home_marketing_status or None,
                    "home_marketing_type": listing.home_marketing_type or None,
                    "listing_url": listing.listing_url or None,
                    "has_open_house": listing.has_open_house or None,
                    "represented": listing.represented or None,
                    "sold_date": listing.sold_date or None,
                    "home_details_url": listing.home_details_url or None,
                    "living_area_value": listing.living_area_value or None,
                    "living_area_units_short": listing.living_area_units_short or None,
                    "mls_logo_src": listing.mls_logo_src or None,
                    "line1": listing.address.line1 if listing.address else None,
                    "line2": listing.address.line2 if listing.address else None,
                    "state_or_province": listing.address.state_or_province if listing.address else None,
                    "city": listing.address.city if listing.address else None,
                    "postal_code": listing.address.postal_code if listing.address else None
                })

                listing_agent_data.append({
                    "listing_id": listing.zpid,
                    "agent_id": agent.encodedzuid
                })

            if listings_all_data:
                stmt = pg_insert(ListingModel).values(listings_all_data).on_conflict_do_nothing()
                await session.execute(stmt)

                stmt = pg_insert(ListingAgentModel).values(listing_agent_data).on_conflict_do_nothing()
                await session.execute(stmt)

                logfire.info(f"Listings inserted for agent: {agent.encodedzuid}")

        except Exception as e:
            await session.rollback()
            logfire.error(f"Error inserting listings for agent {agent.encodedzuid}: {e}")

    async def agent_exists(self, agent: Agent, session: AsyncSession) -> bool:
        """Check if the agent already exists in the database."""
        try:
            logfire.info(f"Checking if agent exists: {agent.encodedzuid}")
            result = await session.execute(
                select(AgentModel.encodedzuid).where(AgentModel.encodedzuid == agent.encodedzuid)
            )
            return result.scalar() is not None
        except Exception as e:
            logfire.error(f"Error checking if agent exists: {e}")
            return

    async def insert_agents(self, agents: List[Agent], city: str, state: str, update_existing: bool = False):
        """ 
        Process agent data into batches and insert into the database.

        If update_existing is True, the agent data is updated if it already exists in the database. This is done by appending the specialties,
          updating the page and ranking. Deletes the old phone, website, and listing data. All of this is done in the prepare methods. The core
            data is updated here.

        If update_existing is False, the agent data is skipped if it already exists in the database, else it is inserted as new data.
        
        If any errors happen during a batch, the batch is rolled back and the process continues with the next batch.
        Edge Case: If errors happen during the insertion of the listings, the agent data is still inserted, but the listings are not.
        """

        async with self.SessionLocal() as session:
            try:
                logfire.info(f"Starting Insertion Process for {city}, {state}")
                if not agents:
                    logfire.error(f"No agents found for {city}, {state}")
                    await self.insert_status(city, state, "ERROR", session)
                    return

                city_id = await self.insert_city(city, state, session)
                if not city_id:
                    logfire.error(f"Failed to insert or find city {city}, {state}")
                    await self.insert_status(city, state, "ERROR", session)
                    return

                batch_size = 300
                for i in range(0, len(agents), batch_size):
                    batch_agents = agents[i:i + batch_size]
                    logfire.info(f"Processing batch {i // batch_size + 1} with {len(batch_agents)} agents")

                    agent_data = []
                    agent_city_data = []
                    phones_data = []
                    websites_data = []

                    for agent in batch_agents:
                        agent_exists = await self.agent_exists(agent, session)
                        if not update_existing and agent_exists:
                            logfire.info(f"Agent already exists in the database, skipping (update_existing Was False): {agent.encodedzuid}")
                            agent_city_data.append(self.prepare_agent_city(agent, city_id))
                            continue
                        else:
                            logfire.info(f"Preparing data for agent: {agent.encodedzuid}")
                            a_data = await self.prepare_individual_agent(agent, city, state, session)
                            if a_data:
                                agent_data.append(a_data)
                            agent_city_data.append(self.prepare_agent_city(agent, city_id))
                            phones_data.extend(await self.prepare_phones(agent, agent_exists, session))
                            websites_data.extend(await self.prepare_websites(agent, agent_exists, session))

                    # from this point, we're guaranteed that the agent data is either new or we're updating it
                    if agent_data:
                        try:
                            logfire.info(f"Inserting batch {i // batch_size + 1} with {len(agent_data)} agents")
                            stmt = pg_insert(AgentModel).values(agent_data).on_conflict_do_update(
                                index_elements=['encodedzuid'],
                                set_={
                                    'business_name': pg_insert(AgentModel).excluded.business_name,                              # not sure why errors r raised here if I use stmt.excluded.{field}
                                    'full_name': pg_insert(AgentModel).excluded.full_name,
                                    'location': pg_insert(AgentModel).excluded.location,
                                    'profile_link': pg_insert(AgentModel).excluded.profile_link,
                                    'sale_count_all_time': pg_insert(AgentModel).excluded.sale_count_all_time,
                                    'sale_count_last_year': pg_insert(AgentModel).excluded.sale_count_last_year,
                                    'sale_price_range_three_year_min': pg_insert(AgentModel).excluded.sale_price_range_three_year_min,
                                    'sale_price_range_three_year_max': pg_insert(AgentModel).excluded.sale_price_range_three_year_max,
                                    'is_team_lead': pg_insert(AgentModel).excluded.is_team_lead,
                                    'is_top_agent': pg_insert(AgentModel).excluded.is_top_agent,
                                    'email': pg_insert(AgentModel).excluded.email,
                                    'ranking': pg_insert(AgentModel).excluded.ranking,
                                    'page': pg_insert(AgentModel).excluded.page,
                                    'specialties': pg_insert(AgentModel).excluded.specialties
                                }
                            )
                            await session.execute(stmt)

                        except Exception as batch_error:
                            logfire.error(f"Error inserting batch {i // batch_size + 1} for {city}, {state}: {batch_error}")
                            await self.insert_status(city, state, "ERROR", session)
                            await session.rollback()
                            continue

                        if agent_city_data:
                            try:
                                logfire.info(f"Inserting agent-city data for batch {i // batch_size + 1}")
                                stmt = pg_insert(AgentCityModel).values(agent_city_data).on_conflict_do_nothing()
                                await session.execute(stmt)

                            except Exception as batch_error:
                                logfire.error(f"Error inserting agent-city data for batch {i // batch_size + 1} for {city}, {state}: {batch_error}")
                                await self.insert_status(city, state, "ERROR", session)
                                await session.rollback()
                                continue

                        if phones_data:
                            try:
                                # new phones (if any) will be accounted for in the prepare_phones method, old phones will be discarded
                                logfire.info(f"Inserting phone data for batch {i // batch_size + 1}")
                                stmt = pg_insert(PhoneModel).values(phones_data).on_conflict_do_nothing()
                                await session.execute(stmt)
                            except Exception as batch_error:
                                logfire.error(f"Error inserting phone data for batch {i // batch_size + 1} for {city}, {state}: {batch_error}")
                                await self.insert_status(city, state, "ERROR", session)
                                await session.rollback()
                                continue

                        if websites_data:
                            try:
                                # new websites (if any) will be accounted for in the prepare_websites method, old websites will be discarded
                                logfire.info(f"Inserting website data for batch {i // batch_size + 1}")
                                stmt = pg_insert(WebsiteModel).values(websites_data).on_conflict_do_nothing()
                                await session.execute(stmt)
                            except Exception as batch_error:
                                logfire.error(f"Error inserting website data for batch {i // batch_size + 1} for {city}, {state}: {batch_error}")
                                await self.insert_status(city, state, "ERROR", session)
                                await session.rollback()
                                continue

                        # commit before inserting listings
                        await session.commit()
                        logfire.info(f"Batch {i // batch_size + 1} committed")

                        # Batch insert listings for EACH agent
                        # new listings (if any) will be accounted for in the insert_listings method, old listings will be discarded
                        logfire.info(f"Inserting listing data for batch {i // batch_size + 1}")
                        try:
                            for agent in batch_agents:
                                await self.insert_listings(agent, agent_exists, session)
                        except Exception as batch_error:
                            logfire.error(f"Error inserting listings for batch {i // batch_size + 1} for {city}, {state}: {batch_error}")
                            await self.insert_status(city, state, "ERROR", session)
                            await session.rollback()
                            continue

                await self.insert_status(city, state, "COMPLETED", session)
                logfire.info(f"Insertion process completed for {city}, {state}")
                logfire.info(f"Batch {i // batch_size + 1} Completed For {city}, {state}")
            except Exception as e:
                logfire.error(f"Error inserting agents for {city}, {state}: {e}")
                await self.insert_status(city, state, "ERROR", session)
                await session.rollback()
