from sqlalchemy import Column, String, Integer
from uploads.h2o.resources import Base_omop 

class Vocabulary(Base_omop):
    __tablename__ = "vocabulary"

    vocabulary_id = Column(String(20), primary_key=True)
    vocabulary_name = Column(String(255), nullable=False)
    vocabulary_reference = Column(String(255), nullable=True)
    vocabulary_version = Column(String(255), nullable=True)
    vocabulary_concept_id = Column(Integer, nullable=True)
