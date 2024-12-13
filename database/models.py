from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, DateTime, Float, ARRAY
from sqlalchemy.orm import relationship, declarative_base
# from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Agent(Base):
    __tablename__ = 'agent'
    
    encodedzuid = Column(String, primary_key=True)
    business_name = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    location = Column(String, nullable=True)
    profile_link = Column(String, nullable=True)
    email = Column(String, nullable=True)
    is_team_lead = Column(Boolean, nullable=True)
    is_top_agent = Column(Boolean, nullable=True)
    sale_count_all_time = Column(Integer, nullable=True)
    sale_count_last_year = Column(Integer, nullable=True)
    sale_price_range_three_year_min = Column(Integer, nullable=True)
    sale_price_range_three_year_max = Column(Integer, nullable=True)
    ranking = Column(Integer, nullable=True)
    page = Column(Integer, nullable=True)
    specialties = Column(ARRAY(String), nullable=True)
    
    cities = relationship('City', secondary='agent_city', back_populates='agents')
    listings = relationship('Listing', secondary='listing_agent', back_populates='agents')
    phones = relationship('Phone', back_populates='agent')
    websites = relationship('Website', back_populates='agent')


class City(Base):
    __tablename__ = 'city'
    
    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    
    agents = relationship('Agent', secondary='agent_city', back_populates='cities')


class AgentCity(Base):
    __tablename__ = 'agent_city'
    
    id = Column(Integer, primary_key=True)
    agent_id = Column(String, ForeignKey('agent.encodedzuid'), nullable=True)
    city_id = Column(Integer, ForeignKey('city.id'), nullable=True)


class Listing(Base):
    __tablename__ = 'listing'
    
    zpid = Column(Integer, primary_key=True)
    type = Column(String, nullable=True)
    home_type = Column(String, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    bathrooms = Column(Float, nullable=True)
    has_open_house = Column(Boolean, nullable=True)
    price = Column(Text, nullable=True)
    price_currency = Column(String, default='USD', nullable=True)
    status = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    brokerage_name = Column(String, nullable=True)
    home_marketing_status = Column(String, nullable=True)
    home_marketing_type = Column(String, nullable=True)
    listing_url = Column(String, nullable=True)
    represented = Column(String, nullable=True)
    sold_date = Column(String, nullable=True)
    home_details_url = Column(String, nullable=True)
    living_area_value = Column(Float, nullable=True)
    living_area_units_short = Column(String, nullable=True)
    mls_logo_src = Column(String, nullable=True)
    line1 = Column(Text, nullable=True)
    line2 = Column(Text, nullable=True)
    state_or_province = Column(Text, nullable=True)
    city = Column(Text, nullable=True)
    postal_code = Column(Text, nullable=True)
    
    agents = relationship('Agent', secondary='listing_agent', back_populates='listings')


class ListingAgent(Base):
    __tablename__ = 'listing_agent'
    
    id = Column(Integer, primary_key=True)
    listing_id = Column(Integer, ForeignKey('listing.zpid'), nullable=True)
    agent_id = Column(String, ForeignKey('agent.encodedzuid'), nullable=True)
    role = Column(String, nullable=True)


class Phone(Base):
    __tablename__ = 'phone'
    
    id = Column(Integer, primary_key=True)
    agent_id = Column(String, ForeignKey('agent.encodedzuid'), nullable=True)
    phone = Column(String, nullable=False)
    type = Column(String, nullable=True)
    
    agent = relationship('Agent', back_populates='phones')


class Status(Base):
    __tablename__ = 'status'
    
    id = Column(Integer, primary_key=True)
    city_id = Column(Integer, ForeignKey('city.id'), nullable=True)
    job_status = Column(String, nullable=True)
    last_updated = Column(DateTime, nullable=True)


class Website(Base):
    __tablename__ = 'website'
    
    id = Column(Integer, primary_key=True)
    agent_id = Column(String, ForeignKey('agent.encodedzuid'), nullable=True)
    website_url = Column(String, nullable=False)
    website_type = Column(String, nullable=True)
    
    
    agent = relationship('Agent', back_populates='websites')
