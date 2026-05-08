from sqlalchemy import Column, Integer, String, Date, Numeric, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class Specimen(Base_omop):
    __tablename__ = 'specimen'

    specimen_id = Column(Integer, primary_key=True)
    person_id = Column(String(50), nullable=False)
    specimen_concept_id = Column(Integer, nullable=False)
    specimen_type_concept_id = Column(Integer, nullable=False)
    specimen_date = Column(Date, nullable=False)
    specimen_datetime = Column(TIMESTAMP, nullable=True)
    quantity = Column(Numeric, nullable=True)
    unit_concept_id = Column(Integer, nullable=True)
    anatomic_site_concept_id = Column(Integer, nullable=True)
    disease_status_concept_id = Column(Integer, nullable=True)
    specimen_source_id = Column(String(50), nullable=True)
    specimen_source_value = Column(String(50), nullable=True)
    unit_source_value = Column(String(50), nullable=True)
    anatomic_site_source_value = Column(String(50), nullable=True)
    disease_status_source_value = Column(String(50), nullable=True)
