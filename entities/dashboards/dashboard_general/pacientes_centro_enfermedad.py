from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Pacientes_centro_enfermedad(Base_dashboard):
    __tablename__ = "pacientes_centro_enfermedad"

    id = Column(Integer, primary_key=True, index=True)
    enfermedad = Column(String(50), nullable=False)
    centro = Column(String(100), nullable=False)
    valor = Column(Numeric, nullable=False)
    cliente= Column(String(10), nullable=False)
