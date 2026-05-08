from sqlalchemy import Column, Integer
from uploads.h2o.resources import Base_omop 

class EpisodeEvent(Base_omop):
    __tablename__ = 'episode_event'

    episode_id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, primary_key=True, index=True)
    episode_event_field_concept_id = Column(Integer, primary_key=True, index=True)
