# entities/procedure_occurrence.py
from sqlalchemy import Column, Integer, String, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class ProcedureOccurrence(Base_omop):
    __tablename__ = 'procedure_occurrence'

    procedure_occurrence_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    procedure_concept_id = Column(Integer, nullable=False)
    procedure_date = Column(Date, nullable=False)
    procedure_datetime = Column(TIMESTAMP, nullable=True)
    procedure_end_date = Column(Date, nullable=True)
    procedure_end_datetime = Column(TIMESTAMP, nullable=True)
    procedure_type_concept_id = Column(Integer, nullable=False)
    modifier_concept_id = Column(Integer, nullable=True)
    quantity = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    procedure_source_value = Column(String(50), nullable=True)
    procedure_source_concept_id = Column(Integer, nullable=True)
    modifier_source_value = Column(String(50), nullable=True)
