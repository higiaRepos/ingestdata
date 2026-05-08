from sqlalchemy import Column, Integer, String, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class VisitOccurrence(Base_omop):
    __tablename__ = "visit_occurrence"

    visit_occurrence_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    visit_concept_id = Column(Integer, nullable=False)
    visit_start_date = Column(Date, nullable=False)
    visit_start_datetime = Column(TIMESTAMP, nullable=True)
    visit_end_date = Column(Date, nullable=False)
    visit_end_datetime = Column(TIMESTAMP, nullable=True)
    visit_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    visit_source_value = Column(String(50), nullable=True)
    visit_source_concept_id = Column(Integer, nullable=True)
    admitted_from_concept_id = Column(Integer, nullable=True)
    admitted_from_source_value = Column(String(50), nullable=True)
    discharged_to_concept_id = Column(Integer, nullable=True)
    discharged_to_source_value = Column(String(50), nullable=True)
    preceding_visit_occurrence_id = Column(Integer, nullable=True)
