from sqlalchemy import Column, Integer, String, Numeric, Date, TIMESTAMP
from uploads.h2o.resources import Base_omop 

class Metadata(Base_omop):
    __tablename__ = 'metadata'

    metadata_id = Column(Integer, primary_key=True, index=True)
    metadata_concept_id = Column(Integer, nullable=False)
    metadata_type_concept_id = Column(Integer, nullable=False)
    name = Column(String(250), nullable=False)
    value_as_string = Column(String(250), nullable=True)
    value_as_concept_id = Column(Integer, nullable=True)
    value_as_number = Column(Numeric, nullable=True)
    metadata_date = Column(Date, nullable=True)
    metadata_datetime = Column(TIMESTAMP, nullable=True)
