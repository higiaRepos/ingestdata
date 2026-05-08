from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, Date
from uploads.h2o.resources import Base_omop 

class Note(Base_omop):
    __tablename__ = 'note'

    note_id = Column(Integer, primary_key=True, index=True)
    person_id = Column(String(50), nullable=False)
    note_date = Column(Date, nullable=False)
    note_datetime = Column(TIMESTAMP, nullable=True)
    note_type_concept_id = Column(Integer, nullable=False)
    note_class_concept_id = Column(Integer, nullable=False)
    note_title = Column(String(250), nullable=True)
    note_text = Column(Text, nullable=False)
    encoding_concept_id = Column(Integer, nullable=False)
    language_concept_id = Column(Integer, nullable=False)
    provider_id = Column(Integer, nullable=True)
    visit_occurrence_id = Column(Integer, nullable=True)
    visit_detail_id = Column(Integer, nullable=True)
    note_source_value = Column(String(50), nullable=True)
    note_event_id = Column(Integer, nullable=True)
    note_event_field_concept_id = Column(Integer, nullable=True)
