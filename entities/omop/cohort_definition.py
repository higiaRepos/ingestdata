# entities/cohort_definition.py
from sqlalchemy import Column, Integer, String, Date, Text
from uploads.h2o.resources import Base_omop 

class CohortDefinition(Base_omop):
    __tablename__ = 'cohort_definition'

    cohort_definition_id = Column(Integer, primary_key=True, index=True)
    cohort_definition_name = Column(String(255), nullable=False)
    cohort_definition_description = Column(Text, nullable=True)
    definition_type_concept_id = Column(Integer, nullable=False)
    cohort_definition_syntax = Column(Text, nullable=True)
    subject_concept_id = Column(Integer, nullable=False)
    cohort_initiation_date = Column(Date, nullable=True)
