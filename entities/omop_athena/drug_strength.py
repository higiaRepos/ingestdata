from sqlalchemy import Column, Integer, Numeric, Date, String, BigInteger
from uploads.h2o.resources import Base_omop_athena 

class DrugStrength(Base_omop_athena):
    __tablename__ = 'drug_strength'

    drug_concept_id = Column(Integer, primary_key=True, index=True)
    ingredient_concept_id = Column(Integer, primary_key=True, index=True)
    amount_value = Column(Numeric, nullable=True)
    amount_unit_concept_id = Column(Integer, nullable=True)
    numerator_value = Column(Numeric, nullable=True)
    numerator_unit_concept_id = Column(Integer, nullable=True)
    denominator_value = Column(Numeric, nullable=True)
    denominator_unit_concept_id = Column(Integer, nullable=True)
    box_size = Column(Integer, nullable=True)
    valid_start_date = Column(Date, nullable=False)
    valid_end_date = Column(Date, nullable=False)
    invalid_reason = Column(String(1), nullable=True)
