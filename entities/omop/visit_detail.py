from sqlalchemy import Column, Integer, String, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class VisitDetail(Base_omop):
    __tablename__ = 'visit_detail'

    visit_detail_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    visit_detail_concept_id = Column(Integer, nullable=False)
    visit_detail_start_date = Column(Date, nullable=False)
    visit_detail_start_datetime = Column(TIMESTAMP, nullable=True)
    visit_detail_end_date = Column(Date, nullable=False)
    visit_detail_end_datetime = Column(TIMESTAMP, nullable=True)
    visit_detail_type_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer, nullable=True)
    care_site_id = Column(Integer, nullable=True)
    visit_detail_source_value = Column(String(50), nullable=True)
    visit_detail_source_concept_id = Column(Integer, nullable=True)
    admitted_from_concept_id = Column(Integer, nullable=True)
    admitted_from_source_value = Column(String(50), nullable=True)
    discharged_to_source_value = Column(String(50), nullable=True)
    discharged_to_concept_id = Column(Integer, nullable=True)
    preceding_visit_detail_id = Column(Integer, nullable=True)
    parent_visit_detail_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=False)
