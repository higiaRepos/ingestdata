from sqlalchemy import Column, String, Integer, ARRAY, Date, Numeric
from uploads.h2o.resources import Base_dashboard 


class Demografia(Base_dashboard):
    __tablename__ = "demografia"

    patient_id = Column(String(50), primary_key=True)
    origen = Column(String(50), primary_key=True)

    gender = Column(String(255), nullable=False)
    age = Column(Integer, nullable=False)
    type_disease = Column(String(255), nullable=False)
    comorbidities = Column(ARRAY(String), nullable=False)
    num_comorbidities = Column(Integer, nullable=False)
    ENROL_DATE = Column(Date, nullable=False)
    DB_001 = Column(ARRAY(String), nullable=False)
    DB_002 = Column(ARRAY(String), nullable=False)
    altura = Column(Numeric, nullable=False)
    peso = Column(Numeric, nullable=False)
    fuma = Column(String(255), nullable=False)
    alcohol = Column(String(255), nullable=False)
    age_group = Column(String(6), nullable=False)
    bmi = Column(Numeric, nullable=False)
    bmi_cat = Column(String(22), nullable=False)
    cliente= Column(String(10), primary_key=True)
    