# entities/cdm_source.py
from sqlalchemy import Column, Integer, String, Date, Text
from uploads.h2o.resources import Base_omop 

class CdmSource(Base_omop):
    __tablename__ = 'cdm_source'

    cdm_source_name = Column(String(255), primary_key=True, index=True)
    cdm_source_abbreviation = Column(String(25), nullable=False)
    cdm_holder = Column(String(255), nullable=False)
    source_description = Column(Text, nullable=True)
    source_documentation_reference = Column(String(255), nullable=True)
    cdm_etl_reference = Column(String(255), nullable=True)
    source_release_date = Column(Date, nullable=False)
    cdm_release_date = Column(Date, nullable=False)
    cdm_version = Column(String(10), nullable=True)
    cdm_version_concept_id = Column(Integer, nullable=False)
    vocabulary_version = Column(String(20), nullable=False)
