# entities/observation.py
from sqlalchemy import Column, Integer, String, Date, Numeric, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class Observation(Base_omop):
    __tablename__ = "observation"

    observation_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    observation_concept_id = Column(Integer, nullable=False)
    observation_date = Column(Date, nullable=False)
    observation_datetime = Column(TIMESTAMP, nullable=True)
    observation_type_concept_id = Column(Integer, nullable=False)
    value_as_number = Column(Numeric, nullable=True)
    value_as_string = Column(String(150), nullable=True)
    value_as_concept_id = Column(Integer, nullable=True)
    qualifier_concept_id = Column(Integer, nullable=True)
    unit_concept_id = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    observation_source_value = Column(String(300), nullable=True)
    observation_source_concept_id = Column(Integer, nullable=True)
    unit_source_value = Column(String(50), nullable=True)
    qualifier_source_value = Column(String(50), nullable=True)
    value_source_value = Column(String(50), nullable=True)
    observation_event_id = Column(Integer, nullable=True)
    obs_event_field_concept_id = Column(Integer, nullable=True)
