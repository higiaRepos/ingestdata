from sqlalchemy import Column, Integer, Numeric, Date, TIMESTAMP, String, Text
from uploads.h2o.resources import Base_omop 

class DrugExposure(Base_omop):
    __tablename__ = 'drug_exposure'

    drug_exposure_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    drug_concept_id = Column(Integer, nullable=False)
    drug_exposure_start_date = Column(Date, nullable=False)
    drug_exposure_start_datetime = Column(TIMESTAMP, nullable=True)
    drug_exposure_end_date = Column(Date, nullable=False)
    drug_exposure_end_datetime = Column(TIMESTAMP, nullable=True)
    verbatim_end_date = Column(Date, nullable=True)
    drug_type_concept_id = Column(Integer, nullable=False)
    stop_reason = Column(String(20), nullable=True)
    refills = Column(Integer, nullable=True)
    quantity = Column(Numeric, nullable=True)
    days_supply = Column(Integer, nullable=True)
    sig = Column(Text, nullable=True)
    route_concept_id = Column(Integer, nullable=True)
    lot_number = Column(String(50), nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    drug_source_value = Column(String(50), nullable=True)
    drug_source_concept_id = Column(Integer, nullable=True)
    route_source_value = Column(String(50), nullable=True)
    dose_unit_source_value = Column(String(50), nullable=True)
