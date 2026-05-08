from sqlalchemy import Column, String, Integer
from uploads.h2o.resources import Base_dashboard 

class Test(Base_dashboard):
    __tablename__ = "test"

    test_id = Column(Integer, primary_key=True, index=True)
    concept_id = Column(Integer, nullable=False)
    vocabulary_version = Column(String(255), nullable=False)
    concept_class_name = Column(String(255), nullable=False)


