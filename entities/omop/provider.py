# entities/provider.py
from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop 

class Provider(Base_omop):
    __tablename__ = 'provider'

    provider_id = Column(Integer, primary_key=True, index=True)
    provider_name = Column(String(255), nullable=True)
    npi = Column(String(20), nullable=True)
    dea = Column(String(20), nullable=True)
    specialty_concept_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    year_of_birth = Column(Integer, nullable=True)
    gender_concept_id = Column(Integer, nullable=True)
    provider_source_value = Column(String(50), nullable=True)
    specialty_source_value = Column(String(50), nullable=True)
    specialty_source_concept_id = Column(Integer, nullable=True)
    gender_source_value = Column(String(50), nullable=True)
    gender_source_concept_id = Column(Integer, nullable=True)
