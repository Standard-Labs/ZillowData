from pydantic import BaseModel, HttpUrl, Field
from enum import Enum


class Website(BaseModel):
    website_type: str | None = Field(default=None, alias="text")
    website_url: HttpUrl | None = Field(alias="url")


class Phones(BaseModel):
    cell: str | None = None
    brokerage: str | None = None
    business: str | None = None


class Address(BaseModel):
    line1: str | None = None
    line2: str | None = None
    city: str | None = None
    state_or_province: str | None = Field(default=None, alias="stateOrProvince")
    postal_code: str | None = Field(default=None, alias="postalCode")


class Listing(BaseModel):
    type: str | None = None

    # Shared fields
    zpid: int | None = None
    address: Address | None = None
    bedrooms: int | None = None
    bathrooms: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    price: str | int | None = None
    price_currency: str | None = "USD"

    # Fields specific to Sale/Rent Listings
    status: str | None = None
    home_type: str | None = None
    brokerage_name: str | None = None
    home_marketing_status: str | None = None
    home_marketing_type: str | None = None
    listing_url: str | None = None
    has_open_house: bool | None = None

    # Fields specific to Past Sales
    represented: str | None = None
    sold_date: str | None = None
    image_alt: str | None = None
    home_details_url: str | None = None
    living_area_value: float | None = None
    living_area_units_short: str | None = None
    mls_logo_src: HttpUrl | None = None


class Agent(BaseModel):
    business_name: str | None = Field(default=None, alias="businessName")
    encodedzuid: str = Field(alias="encodedZuid")  # required
    full_name: str | None = Field(default=None, alias="fullName")
    location: str | None = None
    phoneNumber: str | None = None
    profile_link: str | None = Field(default=None, alias="profileLink")
    sale_count_all_time: int | None = Field(default=None, alias="saleCountAllTime")
    sale_count_last_year: int | None = Field(default=None, alias="saleCountLastYear")
    sale_price_three_year_min: int | None = Field(default=None, alias="salePriceRangeThreeYearMin")
    sale_price_three_year_max: int | None = Field(default=None, alias="salePriceRangeThreeYearMax")
    is_team_lead: bool | None = Field(default=None, alias="isTeamLead")
    is_top_agent: bool | None = Field(default=None, alias="isTopAgent")
    phoneNumbers: Phones | None = None
    email: str | None = None
    forSaleListing: list[Listing] = Field(default_factory=list)
    forRentListing: list[Listing] = Field(default_factory=list)
    pastSales: list[Listing] = Field(default_factory=list)
    websites: list[Website] = Field(default_factory=list)
    specialties: list[str] = Field(default_factory=list)
    ranking: int | None = None
    page: int | None = None


agent_types_default = [
    "listing-agent",
    "buyers-agent",
    "relocation",
    "foreclosure"
]

class JobStatus(Enum):
    NOT_SCRAPED = "Scraping/Insertion Has Not Been Initialized For "
    COMPLETED = "Scraping/Insertion Completed Successfully For "
    PENDING = "Scraping Job Still In Progress For "
    ERROR = "Scraping/Insertion Job Encountered An Error. Try Again For "
    UNKNOWN = "Unknown Status For Scraping/Insertion Job For "
    INTERNAL_ERROR = "Internal Server Error When Checking Status For "

    def message(self, city, state):
        return self.value + f"{city}, {state}" 

class ScrapeJobPayload(BaseModel):
    page_start: int | None = None
    page_end: int | None = None
    update_existing: bool | None = False
    city: str
    state: str
    agent_types: list[str] | None = Field(default_factory=lambda: ["listing-agent", "buyers-agent", "relocation", "foreclosure"])