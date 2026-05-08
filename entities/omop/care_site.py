# entities/care_site.py
from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop

class CareSite(Base_omop):
    __tablename__ = 'care_site'

    care_site_id = Column(Integer, primary_key=True, index=True)
    care_site_name = Column(String(255), nullable=True)
    place_of_service_concept_id = Column(Integer, nullable=True)
    location_id = Column(Integer, nullable=True)
    care_site_source_value = Column(String(50), nullable=True)
    place_of_service_source_value = Column(String(50), nullable=True)

