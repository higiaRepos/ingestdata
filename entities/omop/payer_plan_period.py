# entities/payer_plan_period.py
from sqlalchemy import Column, Integer, Date, String
from uploads.h2o.resources import Base_omop 

class PayerPlanPeriod(Base_omop):
    __tablename__ = "payer_plan_period"

    payer_plan_period_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    payer_plan_period_start_date = Column(Date, nullable=False)
    payer_plan_period_end_date = Column(Date, nullable=False)
    payer_concept_id = Column(Integer, nullable=True)
    payer_source_value = Column(String(50), nullable=True)
    payer_source_concept_id = Column(Integer, nullable=True)
    plan_concept_id = Column(Integer, nullable=True)
    plan_source_value = Column(String(50), nullable=True)
    plan_source_concept_id = Column(Integer, nullable=True)
    sponsor_concept_id = Column(Integer, nullable=True)
    sponsor_source_value = Column(String(50), nullable=True)
    sponsor_source_concept_id = Column(Integer, nullable=True)
    family_source_value = Column(String(50), nullable=True)
    stop_reason_concept_id = Column(Integer, nullable=True)
    stop_reason_source_value = Column(String(50), nullable=True)
    stop_reason_source_concept_id = Column(Integer, nullable=True)
