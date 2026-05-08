from sqlalchemy import Column, Integer, Date, TIMESTAMP, String
from uploads.h2o.resources import Base_omop 

class Death(Base_omop):
    __tablename__ = 'death'

    person_id = Column(String(50), primary_key=True, index=True)
    death_date = Column(Date, nullable=False)
    death_datetime = Column(TIMESTAMP, nullable=True)
    death_type_concept_id = Column(Integer, nullable=True)
    cause_concept_id = Column(Integer, nullable=True)
    cause_source_value = Column(String(50), nullable=True)
    cause_source_concept_id = Column(Integer, nullable=True)
