from sqlalchemy import Column, Integer
from uploads.h2o.resources import Base_omop 

class FactRelationship(Base_omop):
    __tablename__ = 'fact_relationship'

    domain_concept_id_1 = Column(Integer, primary_key=True, index=True)
    fact_id_1 = Column(Integer, primary_key=True, index=True)
    domain_concept_id_2 = Column(Integer, primary_key=True, index=True)
    fact_id_2 = Column(Integer, primary_key=True, index=True)
    relationship_concept_id = Column(Integer, primary_key=True, index=True)
