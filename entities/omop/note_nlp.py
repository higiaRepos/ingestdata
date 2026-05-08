from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, Date
from uploads.h2o.resources import Base_omop 

class NoteNlp(Base_omop):
    __tablename__ = 'note_nlp'

    note_nlp_id = Column(Integer, primary_key=True, index=True)
    note_id = Column(Integer, nullable=False)
    section_concept_id = Column(Integer, nullable=True)
    snippet = Column(String(250), nullable=True)
    offset = Column(String(50), nullable=True)
    lexical_variant = Column(String(250), nullable=False)
    note_nlp_concept_id = Column(Integer, nullable=True)
    note_nlp_source_concept_id = Column(Integer, nullable=True)
    nlp_system = Column(String(250), nullable=True)
    nlp_date = Column(Date, nullable=False)
    nlp_datetime = Column(TIMESTAMP, nullable=True)
    term_exists = Column(String(1), nullable=True)
    term_temporal = Column(String(50), nullable=True)
    term_modifiers = Column(String(2000), nullable=True)
