from sqlalchemy import Column, Integer, Numeric, Date, String
from uploads.h2o.resources import Base_omop 

class DoseEra(Base_omop):
    __tablename__ = 'dose_era'

    dose_era_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    unit_concept_id = Column(Integer, nullable=False)
    dose_value = Column(Numeric, nullable=False)
    dose_era_start_date = Column(Date, nullable=False)
    dose_era_end_date = Column(Date, nullable=False)
