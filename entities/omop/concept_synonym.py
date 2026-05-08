# entities/concept_synonym.py
from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop 

class ConceptSynonym(Base_omop):
    __tablename__ = 'concept_synonym'

    concept_id = Column(Integer, primary_key=True)
    concept_synonym_name = Column(String(1000), primary_key=True)
    language_concept_id = Column(Integer, primary_key=True)
