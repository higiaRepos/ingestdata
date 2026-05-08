from sqlalchemy import Column, Integer, Date, String
from uploads.h2o.resources import Base_omop 

class ConditionEra(Base_omop):
    __tablename__ = 'condition_era'

    condition_era_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    condition_concept_id = Column(Integer, nullable=False)
    condition_era_start_date = Column(Date, nullable=False)
    condition_era_end_date = Column(Date, nullable=False)
    condition_occurrence_count = Column(Integer, nullable=True)
