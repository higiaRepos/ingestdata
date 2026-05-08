# entities/concept_relationship.py
from sqlalchemy import Column, Integer, String, Date
from uploads.h2o.resources import Base_omop 

class ConceptRelationship(Base_omop):
    __tablename__ = 'concept_relationship'

    concept_id_1 = Column(Integer, primary_key=True)
    concept_id_2 = Column(Integer, primary_key=True)
    relationship_id = Column(String(20), primary_key=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1), nullable=True)
