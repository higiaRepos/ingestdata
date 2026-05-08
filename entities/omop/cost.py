from sqlalchemy import Column, Integer, String, Numeric, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class Cost(Base_omop):
    __tablename__ = 'cost'

    cost_id = Column(Integer, primary_key=True, index=True)
    cost_event_id = Column(Integer, nullable=False)
    cost_domain_id = Column(String(20), nullable=False)
    cost_type_concept_id = Column(Integer, nullable=False)
    currency_concept_id = Column(Integer, nullable=True)
    total_charge = Column(Numeric, nullable=True)
    total_cost = Column(Numeric, nullable=True)
    total_paid = Column(Numeric, nullable=True)
    paid_by_payer = Column(Numeric, nullable=True)
    paid_by_patient = Column(Numeric, nullable=True)
    paid_patient_copay = Column(Numeric, nullable=True)
    paid_patient_coinsurance = Column(Numeric, nullable=True)
    paid_patient_deductible = Column(Numeric, nullable=True)
    paid_by_primary = Column(Numeric, nullable=True)
    paid_ingredient_cost = Column(Numeric, nullable=True)
    paid_dispensing_fee = Column(Numeric, nullable=True)
    payer_plan_period_id = Column(Integer, nullable=True)
    amount_allowed = Column(Numeric, nullable=True)
    revenue_code_concept_id = Column(Integer, nullable=True)
    revenue_code_source_value = Column(String(50), nullable=True)
    drg_concept_id = Column(Integer, nullable=True)
    drg_source_value = Column(String(3), nullable=True)
