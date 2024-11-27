from typing import List
from database.client import SupabaseClient
from scraper.models import Agent, Address

class Inserter:
    def __init__(self, db_client: SupabaseClient):
        self.db_client = db_client

    def insert_city(self, city: str, state: str, agent: Agent):
        """
        Insert city into the city table if it does not exist, and update the junction table with the agent and city.
        This will upsert the agent-city relation
        """
        try:
            city = city.upper()
            state = state.upper()

            city_id = self.db_client.get_city_id(city, state)
            if city_id is None:
                city_data = {'city': city, 'state': state}
                city_id = self.db_client.insert_data(table_name="city", data=city_data)

            if self.db_client.get_agent(agent.encodedzuid) is not None:
                self.db_client.insert_data(
                    table_name="agent_city",
                    data={'city_id': city_id, 'agent_id': agent.encodedzuid},
                    on_conflict="city_id, agent_id"
                )
            else:
                print(f"Agent {agent.encodedzuid} does not exist, cannot insert city.")
        except Exception as e:
            print(f"Error inserting city {city}, {state} for agent: {e}")

    def insert_status(self, city: str, state: str, status: str):
        try:
            city_id = self.db_client.get_city_id(city, state)
            if city_id is not None:
                status_data = {'city_id': city_id, 'job_status': status, "last_updated": "now()"}
                self.db_client.insert_data(table_name="status", data=status_data, on_conflict="city_id")
            else:
                city_data = {'city': city.upper(), 'state': state.upper()}
                self.db_client.insert_data(table_name="city", data=city_data)
                status_data = {'city_id': self.db_client.get_city_id(city, state), 'job_status': status, "last_updated": "now()"}
                self.db_client.insert_data(table_name="status", data=status_data, on_conflict="city_id")
        except Exception as e:
            print(f"Error inserting status for {city}, {state}: {e}")

    def insert_individual_agent(self, agent: Agent, city: str, state: str):
        # Will function for both new agents, and updating existing agents
        # The city is updated in the agent_city junction table in insert_city

        try:
            page_num = agent.page
            specialties = agent.specialties
            ranking = agent.ranking
            og_agent_data = self.db_client.get_agent(agent.encodedzuid)
            if og_agent_data:
                page_num = min(agent.page, og_agent_data.get('page'))
                specialties = list(set(og_agent_data.get('specialties')).union(set(agent.specialties)))
                ranking = og_agent_data.get('ranking')
            agent_data = {
                'encodedzuid': agent.encodedzuid,
                'business_name': agent.business_name,
                'full_name': agent.full_name,
                'email': agent.email,
                'location': agent.location,
                'profile_link': agent.profile_link,
                'is_team_lead': agent.is_team_lead,
                'is_top_agent': agent.is_top_agent,
                'sale_count_all_time': agent.sale_count_all_time,
                'sale_count_last_year': agent.sale_count_last_year,
                'sale_price_range_three_year_min': agent.sale_price_three_year_min,
                'sale_price_range_three_year_max': agent.sale_price_three_year_max,
                'ranking': ranking,
                'page': page_num,
                'specialties': specialties
            }
            self.db_client.insert_data(table_name="agent", data=agent_data, on_conflict="encodedzuid")
        except Exception as e:
            print(f"Error inserting agent {agent.encodedzuid} for {city}, {state}: {e}")

    def insert_phones(self, agent: Agent):
        try:
            phone_data = {}
            if agent.phoneNumber:
                phone_data['primary'] = agent.phoneNumber
            if agent.phoneNumbers.cell:
                phone_data['cell'] = agent.phoneNumbers.cell
            if agent.phoneNumbers.brokerage:
                phone_data['brokerage'] = agent.phoneNumbers.brokerage
            if agent.phoneNumbers.business:
                phone_data['business'] = agent.phoneNumbers.business

            for phone_type, phone in phone_data.items():
                self.db_client.insert_data(table_name="phone", data={'phone': phone, 'agent_id': agent.encodedzuid,
                                                                     'type': phone_type}, on_conflict="phone, agent_id")
        except Exception as e:
            print(f"Error inserting phone numbers for agent {agent.encodedzuid}: {e}")

    def insert_websites(self, agent: Agent):
        for website in agent.websites:
            try:
                website_data = {'agent_id': agent.encodedzuid, 'website_url':str(website.website_url),
                                'website_type': website.website_type}
                self.db_client.insert_data(table_name="website", data=website_data, on_conflict="agent_id, website_url")
            except Exception as e:
                print(f"Error inserting website for agent {agent.encodedzuid}: {e}")

    def insert_address(self, address: Address, agent_id: str):
        try:
            address_data = {
                'line1': address.line1,
                'line2': address.line2,
                'state_or_province': address.state_or_province,
                'city': address.city,
                'postal_code': address.postal_code,
            }
            address_id = self.db_client.insert_data(table_name="address", data=address_data).data[0].get('id')
            return address_id
        except Exception as e:
            print(f"Error inserting address {address} for agent {agent_id}: {e}")
            return None

    def insert_listings(self, agent: Agent):
        try:
            for listing in agent.pastSales + agent.forRentListing + agent.forSaleListing:
                address_id = self.insert_address(listing.address, agent.encodedzuid)
                if address_id:
                    listing_data = {
                        'type': listing.type,
                        'zpid': listing.zpid,
                        'address_id': address_id,
                        'bedrooms': listing.bedrooms,
                        'bathrooms': listing.bathrooms,
                        'latitude': listing.latitude,
                        'longitude': listing.longitude,
                        'price': str(listing.price),
                        'price_currency': listing.price_currency,
                        'status': listing.status,
                        'home_type': listing.home_type,
                        'brokerage_name': listing.brokerage_name,
                        'home_marketing_status': listing.home_marketing_status,
                        'home_marketing_type': listing.home_marketing_type,
                        'listing_url': str(listing.listing_url),
                        'has_open_house': listing.has_open_house,
                        'represented': listing.represented,
                        'sold_date': listing.sold_date,
                        'home_details_url': str(listing.home_details_url),
                        'living_area_value': listing.living_area_value,
                        'living_area_units_short': listing.living_area_units_short,
                        'mls_logo_src': str(listing.mls_logo_src)
                    }
                    response = self.db_client.insert_data(table_name="listing", data=listing_data, on_conflict="zpid")
                    if response.data[0].get('zpid'):
                        self.db_client.insert_data(table_name="listing_agent",
                                                   data={'agent_id': agent.encodedzuid, 'listing_id': listing.zpid},
                                                   on_conflict="agent_id, listing_id")
                    else:
                        print(f"Error inserting listing {listing.zpid} for agent {agent.encodedzuid} Into agent_listing"
                              f"table")
                else:
                    print(f"Error inserting listing {listing.zpid} for agent {agent.encodedzuid}: Address could not "
                          f"be inserted")
        except Exception as e:
            print(f"Error inserting listings for agent {agent.encodedzuid}: {e}")

    def insert_agents(self, agents: List[Agent] | None, city: str, state: str):
        try:
            if not agents:
                self.insert_status(city, state, "ERROR")
                return

            for agent in agents:
                self.insert_individual_agent(agent, city, state)
                self.insert_city(city, state, agent)
                self.insert_phones(agent)
                self.insert_websites(agent)
                self.insert_listings(agent)

            self.insert_status(city, state, "COMPLETED")

        except Exception as e:
            print(f"Error inserting agents for {city}, {state}: {e}")
            self.insert_status(city, state, "ERROR")

