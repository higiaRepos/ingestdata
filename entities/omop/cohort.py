# entities/cohort.py
from sqlalchemy import Column, Integer, Date
from uploads.h2o.resources import Base_omop 

class Cohort(Base_omop):
    __tablename__ = 'cohort'

    cohort_definition_id = Column(Integer, primary_key=True, index=True)
    subject_id = Column(Integer, nullable=False)
    cohort_start_date = Column(Date, nullable=False)
    cohort_end_date = Column(Date, nullable=False)
