from sqlalchemy import Column, Integer, String, Numeric
from uploads.h2o.resources import Base_omop 

class Location(Base_omop):
    __tablename__ = 'location'

    location_id = Column(Integer, primary_key=True, index=True)
    address_1 = Column(String(50), nullable=True)
    address_2 = Column(String(50), nullable=True)
    city = Column(String(50), nullable=True)
    state = Column(String(10), nullable=True)
    zip = Column(String(9), nullable=True)
    county = Column(String(20), nullable=True)
    location_source_value = Column(String(50), nullable=True)
    country_concept_id = Column(Integer, nullable=True)
    country_source_value = Column(String(80), nullable=True)
    latitude = Column(Numeric, nullable=True)
    longitude = Column(Numeric, nullable=True)
