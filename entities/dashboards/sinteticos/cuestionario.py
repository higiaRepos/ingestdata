from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Cuestionario(Base_dashboard):
    __tablename__ = "cuestionario"

    cuestionario_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    origen = Column(String(50), primary_key=True)

    patient_id =  Column(String(50), nullable=False)
    type = Column(String(255), nullable=False)
    cuestion = Column(String(300), nullable=False)
    answer = Column(String(255), nullable=False)
    numeric_answer = Column(Integer, nullable=False)
    cuestion_date = Column(Date, nullable=False)
    age = Column(Integer, nullable=False)
    gender = Column(String(255), nullable=False)
    type_disease = Column(String(255), nullable=False)
    general_type = Column(String(15), nullable=False)
    type_info = Column(String(255), nullable=False)
    cliente= Column(String(10), primary_key=True)