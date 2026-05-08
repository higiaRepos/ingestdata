from sqlalchemy import Column, Integer, String, Date
from uploads.h2o.resources import Base_omop 

class Concept(Base_omop):
    __tablename__ = 'concept'

    concept_id = Column(Integer, primary_key=True, index=True)
    concept_name = Column(String(300), nullable=False)
    domain_id = Column(String(20), nullable=False)
    vocabulary_id = Column(String(20), nullable=False)
    concept_class_id = Column(String(20), nullable=False)
    standard_concept = Column(String(1), nullable=True)
    concept_code = Column(String(50), nullable=False)
    valid_start_date = Column(Date, nullable=True)
    valid_end_date = Column(Date, nullable=True)
    invalid_reason = Column(String(1), nullable=True)
