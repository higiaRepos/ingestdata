# entities/concept_class.py
from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop 

class ConceptClass(Base_omop):
    __tablename__ = 'concept_class'

    concept_class_id = Column(String(20), primary_key=True, index=True)
    concept_class_name = Column(String(255), nullable=False)
    concept_class_concept_id = Column(Integer, nullable=True)
