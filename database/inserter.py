import multiprocessing
from typing import List
from database.client import SupabaseClient
from scraper.models import Agent, Address

class Inserter:
    def __init__(self, db_client: SupabaseClient):
        self.db_client = db_client

    def prepare_agent_city(self, agent: Agent, city: str, state: str, city_id: str):
        
        return {'city_id': city_id, 'agent_id': agent.encodedzuid}
    
    def insert_city(self, city: str, state: str):
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
                insertion = self.db_client.insert_data(table_name="city", data=city_data)
                return insertion.data[0].get('id')
            else:
                return city_id
        except Exception as e:
            print(f"Error inserting city {city}, {state}: {e}")
            return None

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

    def prepare_individual_agent(self, agent: Agent, city: str, state: str):
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
            return agent_data

        except Exception as e:
            print(f"Error inserting agent {agent.encodedzuid} for {city}, {state}: {e}")
            return []

    def prepare_phones(self, agent: Agent):
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

        # enforcing uniqueness of phone number and agent pair 
        # only have to do it here (even w constraint) because we are batch inserting
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
            return phones
        except Exception as e:
            print(f"Error inserting phone numbers for agent {agent.encodedzuid}: {e}")
            return []

    def prepare_websites(self, agent: Agent):

        # enforcing uniqueness of website url and agent pair 
        # only have to do it here because we are batch inserting
        websites = []
        unique_websites = set()
        for website in agent.websites:
            try:
                if website.website_url in unique_websites:
                    continue
                unique_websites.add(website.website_url)
                website_data = {'agent_id': agent.encodedzuid, 'website_url':str(website.website_url),
                                'website_type': website.website_type}
                websites.append(website_data)
                # self.db_client.insert_data(table_name="website", data=website_data, on_conflict="agent_id, website_url")
            except Exception as e:
                print(f"Error inserting website for agent {agent.encodedzuid}: {e}")
                return []
        return websites

    def insert_address(self, addresses: list[Address], agent_id: str):
        try:
            address_data = []
            for address in addresses:
                address_data.append({
                    'line1': address.line1,
                    'line2': address.line2,
                    'state_or_province': address.state_or_province,
                    'city': address.city,
                    'postal_code': address.postal_code
                })
                return address_data
        except Exception as e:
            print(f"Error preparing addresses for agent {agent_id}: {e}")
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
            
            city_id = self.insert_city(city, state)

            agent_data = []
            agent_city_data = []
            phones_data = []
            websites_data = []
            address_data = []
            listing_data = []
            for agent in agents:
                agent_data.append(self.prepare_individual_agent(agent, city, state))
                agent_city_data.append(self.prepare_agent_city(agent, city, state, city_id))
                phones_data.extend(self.prepare_phones(agent))
                websites_data.extend(self.prepare_websites(agent))
                # skip listings + addresses for now 
                
                # address_data.extend(self.insert_address(agent.forRentListing + agent.forSaleListing + agent.pastSales, agent.encodedzuid))


            # batch insert data
            self.db_client.insert_data(table_name="agent", data=agent_data, on_conflict="encodedzuid")
            self.db_client.insert_data(table_name="agent_city", data=agent_city_data, on_conflict="city_id, agent_id")
            self.db_client.insert_data(table_name="phone", data=phones_data, on_conflict="phone, agent_id")
            self.db_client.insert_data(table_name="website", data=websites_data, on_conflict="agent_id, website_url")
            # address_response = self.db_client.insert_data(table_name="address", data=address_data)


            self.insert_status(city, state, "COMPLETED")

        except Exception as e:
            print(f"Error inserting agents for {city}, {state}: {e}")
            self.insert_status(city, state, "ERROR")






             # def process_agents_batch(self, agents_batch: List[Agent], city: str, state: str, city_id: int):
    
    
    
    
    
    # mulit processing testing 
    
    #     try:
    #         agent_data = []
    #         agent_city_data = []
    #         phones_data = []
    #         websites_data = []
    #         for agent in agents_batch:
    #             agent_data.append(self.insert_individual_agent(agent, city, state))
    #             agent_city_data.append(self.insert_agent_city(agent, city, state, city_id))
    #             phones_data.extend(self.insert_phones(agent))
    #             websites_data.extend(self.insert_websites(agent))
            
    #         self.db_client.insert_data(table_name="agent", data=agent_data, on_conflict="encodedzuid")
    #         self.db_client.insert_data(table_name="agent_city", data=agent_city_data, on_conflict="city_id, agent_id")
    #         self.db_client.insert_data(table_name="phone", data=phones_data, on_conflict="phone, agent_id")
    #         self.db_client.insert_data(table_name="website", data=websites_data, on_conflict="agent_id, website_url")

    #         # skip listings + addresses for now 

    #     except Exception as e:
    #         print(f"Error processing agent batch for {city}, {state}: {e}")



    # def insert_agents(self, agents: List[Agent] | None, city: str, state: str):
    #     try:
    #         if not agents:
    #             self.insert_status(city, state, "ERROR")
    #             return
            
    #         city_id = self.insert_city(city, state)
            
    #         batch_size = 50
    #         agent_batches = [agents[i:i + batch_size] for i in range(0, len(agents), batch_size)]
            
    #         with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    #             pool.starmap(self.process_agents_batch, [(batch, city, state, city_id) for batch in agent_batches])

    #         self.insert_status(city, state, "COMPLETED")

    #     except Exception as e:
    #         print(f"Error inserting agents for {city}, {state}: {e}")
    #         self.insert_status(city, state, "ERROR")
