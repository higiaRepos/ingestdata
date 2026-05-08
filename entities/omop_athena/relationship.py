# entities/relationship.py
from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop_athena 

class Relationship(Base_omop_athena):
    __tablename__ = 'relationship'

    relationship_id = Column(String(20), primary_key=True, index=True)
    relationship_name = Column(String(255), nullable=False)
    is_hierarchical = Column(String(1), nullable=False)
    defines_ancestry = Column(String(1), nullable=False)
    reverse_relationship_id = Column(String(20), nullable=False)
    relationship_concept_id = Column(Integer, nullable=False)
