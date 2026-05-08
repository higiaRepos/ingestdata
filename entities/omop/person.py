# entities/person.py
from sqlalchemy import Column, Integer, String, Date
from uploads.h2o.resources import Base_omop 

class Person(Base_omop):
    __tablename__ = 'person'

    person_id = Column(String(50), primary_key=True, index=True)
    gender_concept_id = Column(Integer, nullable=False)
    year_of_birth = Column(Integer, nullable=False)
    month_of_birth = Column(Integer, nullable=True)
    day_of_birth = Column(Integer, nullable=True)
    birth_datetime = Column(Date, nullable=True)
    race_concept_id = Column(Integer, nullable=False)
    ethnicity_concept_id = Column(Integer, nullable=False)
    location_id = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(String(50), nullable=False)
    person_source_value = Column(String(100), nullable=True)
    gender_source_value = Column(String(50), nullable=True)
    gender_source_concept_id = Column(Integer, nullable=True)
    race_source_value = Column(String(50), nullable=True)
    race_source_concept_id = Column(Integer, nullable=True)
    ethnicity_source_value = Column(String(50), nullable=True)
    ethnicity_source_concept_id = Column(Integer, nullable=True)

    



