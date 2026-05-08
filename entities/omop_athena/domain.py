from sqlalchemy import Column, Integer, String
from uploads.h2o.resources import Base_omop_athena 

class Domain(Base_omop_athena):
    __tablename__ = 'domain'

    domain_id = Column(String(20), primary_key=True, index=True)
    domain_name = Column(String(255), nullable=False)
    domain_concept_id = Column(Integer, nullable=True)
