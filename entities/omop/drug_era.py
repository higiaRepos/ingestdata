from sqlalchemy import Column, Integer, Numeric, Date, String
from uploads.h2o.resources import Base_omop 

class DrugEra(Base_omop):
    __tablename__ = 'drug_era'

    drug_era_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    drug_era_start_date = Column(Date, nullable=False)
    drug_era_end_date = Column(Date, nullable=False)
    drug_exposure_count = Column(Integer, nullable=True)
    gap_days = Column(Integer, nullable=True)
