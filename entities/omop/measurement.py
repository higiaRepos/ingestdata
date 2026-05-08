from sqlalchemy import Column, Integer, String, Numeric, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class Measurement(Base_omop):
    __tablename__ = 'measurement'

    measurement_id = Column(Integer, primary_key=True) 
    person_id = Column(String(50), nullable=False)
    measurement_concept_id = Column(Integer, nullable=False)
    measurement_date = Column(Date, nullable=False)
    measurement_datetime = Column(TIMESTAMP, nullable=True)
    measurement_time = Column(String(10), nullable=True)
    measurement_type_concept_id = Column(Integer, nullable=False)
    operator_concept_id = Column(Integer, nullable=True)
    value_as_number = Column(Numeric, nullable=True)
    value_as_concept_id = Column(Integer, nullable=True)
    unit_concept_id = Column(Integer, nullable=True)
    range_low = Column(Numeric, nullable=True)
    range_high = Column(Numeric, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    measurement_source_value = Column(String(50), nullable=True)
    measurement_source_concept_id = Column(Integer, nullable=True)
    unit_source_value = Column(String(50), nullable=True)
    unit_source_concept_id = Column(Integer, nullable=True)
    value_source_value = Column(String(50), nullable=True)
    measurement_event_id = Column(Integer, nullable=True)
    meas_event_field_concept_id = Column(Integer, nullable=True)
