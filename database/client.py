import logfire
from supabase import create_client
from scraper.models import JobStatus

class SupabaseClient:
    def __init__(self, url, key):
        self.client = create_client(url, key)

    def insert_data(self, table_name, data, on_conflict=None):
        """
        Upsert data into the specified table, with optional on_conflict clause
        Note: multiple on_conflict clauses must be passed as a single string like "column1, column2"
        """

        try:
            if on_conflict is not None:
                query = self.client.table(table_name).upsert(data, on_conflict=on_conflict)
            else:
                query = self.client.table(table_name).upsert(data)
            response = query.execute()

            return response
        except Exception as e:
            logfire.error(f"Error inserting data into {table_name}: {e}")
            return None

    def fetch_data(self, table_name, filters=None, select_data="*"):
        """ Fetch data from the specified table with optional filters and select_data """

        if filters is None:
            filters = {}
        try:
            query = self.client.table(table_name).select(select_data)
            for key, value in filters.items():
                query = query.eq(key, value)
            return query.execute()
        except Exception as e:
            logfire.error(f"Error fetching data from {table_name}: {e}")
            return None

    def check_status(self, city, state):
        """ Check the status of the scraping job for the specified city and state """

        try:
            city_id = self.get_city_id(city, state)
            if city_id is None:
               return JobStatus.NOT_SCRAPED

            response = self.fetch_data("status", filters={"city_id": city_id}, select_data="city_id, job_status")

            if not response.data:
                # This should not ever happen, since the city_id should always exist in the status table if the city has
                # an entry in the city table. The presence of an entry in the city table guarantees an entry in the
                # status table and vice versa.
                logfire.error(f"City ID {city_id} does not exist in the status table.")
                return JobStatus.NOT_SCRAPED

            job_status = response.data[0].get("job_status")

            if job_status == "COMPLETED":
                # Job has previously been completed successfully
                return JobStatus.COMPLETED

            elif job_status == "PENDING":
                # Job is still in progress
                return JobStatus.PENDING

            elif job_status == "ERROR":
                # Job encountered an error previously
                return JobStatus.ERROR

            else:
                # Unknown status
                return JobStatus.UNKNOWN

        except Exception as e:
            logfire.error(f"Error checking status for {city}, {state}: {e}")
            return JobStatus.INTERNAL_ERROR

    def get_city_id(self, city, state):
        """ Get the city_id for the specified city and state """
        try:
            response = self.fetch_data("city", filters={"city": city.upper(), "state": state.upper()}, select_data="id")
            if response.data:
                return response.data[0].get("id")
            else:
                return None
        except Exception as e:
            logfire.error(f"Error getting city ID for {city}, {state}: {e}")
            return None

    def get_agent(self, encodedZuid: str):
        """ Returns the agent data for the specified encodedzuid, if it exists, else returns None """
        try:
            response = self.fetch_data("agent", filters={"encodedzuid": encodedZuid})
            if response.data:
                return response.data[0]
            else:
                return None
        except Exception as e:
            logfire.error(f"Error getting agent data for {encodedZuid}: {e}")
            return False

    def get_agent_cities(self, encodedZuid: str, city_id: int):
        """ Get the city ID's associated with the agent (queries the agent_city junction table) """
        try:
            response = self.fetch_data("agent_city", filters={"agent_id": encodedZuid, "city_id": city_id})
            if response.data:
                return [item.get("city_id") for item in response.data]
            else:
                return None
        except Exception as e:
            logfire.error(f"Error getting agent cities for {encodedZuid}: {e}")
            return None

