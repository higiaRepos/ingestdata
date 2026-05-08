from sqlalchemy import Column, Integer, Date, TIMESTAMP, String
from uploads.h2o.resources import Base_omop 

class Episode(Base_omop):
    __tablename__ = 'episode'

    episode_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    episode_concept_id = Column(Integer, nullable=False)
    episode_start_date = Column(Date, nullable=False)
    episode_start_datetime = Column(TIMESTAMP, nullable=True)
    episode_end_date = Column(Date, nullable=True)
    episode_end_datetime = Column(TIMESTAMP, nullable=True)
    episode_parent_id = Column(Integer, nullable=True)
    episode_number = Column(Integer, nullable=True)
    episode_object_concept_id = Column(Integer, nullable=False)
    episode_type_concept_id = Column(Integer, nullable=False)
    episode_source_value = Column(String(50), nullable=True)
    episode_source_concept_id = Column(Integer, nullable=True)
