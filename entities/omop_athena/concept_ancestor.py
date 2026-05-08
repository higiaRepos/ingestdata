# entities/concept_ancestor.py
from sqlalchemy import Column, Integer
from uploads.h2o.resources import Base_omop_athena 

class ConceptAncestor(Base_omop_athena):
    __tablename__ = 'concept_ancestor'

    ancestor_concept_id = Column(Integer, primary_key=True, index=True)
    descendant_concept_id = Column(Integer, primary_key=True, index=True)
    min_levels_of_separation = Column(Integer, nullable=False)
    max_levels_of_separation = Column(Integer, nullable=False)
