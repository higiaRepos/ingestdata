from sqlalchemy import Column, Integer, String, Numeric, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class DeviceExposure(Base_omop):
    __tablename__ = 'device_exposure'

    device_exposure_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    device_concept_id = Column(Integer, nullable=False)
    device_exposure_start_date = Column(Date, nullable=False)
    device_exposure_start_datetime = Column(TIMESTAMP, nullable=True)
    device_exposure_end_date = Column(Date, nullable=True)
    device_exposure_end_datetime = Column(TIMESTAMP, nullable=True)
    device_type_concept_id = Column(Integer, nullable=False)
    unique_device_id = Column(String(255), nullable=True)
    production_id = Column(String(255), nullable=True)
    quantity = Column(Integer, nullable=True)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    device_source_value = Column(String(50), nullable=True)
    device_source_concept_id = Column(Integer, nullable=True)
    unit_concept_id = Column(Integer, nullable=True)
    unit_source_value = Column(String(50), nullable=True)
    unit_source_concept_id = Column(Integer, nullable=True)
