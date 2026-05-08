# entities/observation_period.py
from sqlalchemy import Column, Integer, Date, String
from uploads.h2o.resources import Base_omop 

class ObservationPeriod(Base_omop):
    __tablename__ = "observation_period"

    observation_period_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    observation_period_start_date = Column(Date, nullable=False)
    observation_period_end_date = Column(Date, nullable=False)
    period_type_concept_id = Column(Integer, nullable=False)
