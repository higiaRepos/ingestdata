from sqlalchemy import Column, Integer, String, Date
from uploads.h2o.resources import Base_omop 

class SourceToConceptMap(Base_omop):
    __tablename__ = 'source_to_concept_map'

    source_code = Column(String(50), primary_key=True)
    source_concept_id = Column(Integer, nullable=False)
    source_vocabulary_id = Column(String(20), primary_key=True)
    source_code_description = Column(String(255), nullable=True)
    target_concept_id = Column(Integer, nullable=False)
    target_vocabulary_id = Column(String(20), nullable=False)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1), nullable=True)
