from sqlalchemy import Column, String, Integer, ARRAY, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Tratamiento(Base_dashboard):
    __tablename__ = "tratamiento"

    patient_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    origen = Column(String(50), primary_key=True)
    tratamiento = Column(String(255), primary_key=True)
    fecha_inicio = Column(Date, primary_key=True)
    estado = Column(String(50), nullable=False)
    cliente= Column(String(10), nullable=False)
