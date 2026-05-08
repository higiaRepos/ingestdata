from sqlalchemy import Column, Integer, Date, String, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class ConditionOccurrence(Base_omop):
    __tablename__ = 'condition_occurrence'

    condition_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    condition_concept_id = Column(Integer, nullable=False)
    condition_start_date = Column(Date, nullable=False)
    condition_start_datetime = Column(TIMESTAMP, nullable=True)
    condition_end_date = Column(Date, nullable=True)
    condition_end_datetime = Column(TIMESTAMP, nullable=True)
    condition_type_concept_id = Column(Integer, nullable=False)
    condition_status_concept_id = Column(Integer, nullable=True)
    stop_reason = Column(String(20), nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    condition_source_value = Column(String(50), nullable=True)
    condition_source_concept_id = Column(Integer, nullable=True)
    condition_status_source_value = Column(String(50), nullable=True)
