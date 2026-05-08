from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Respondedores_centro_enfermedad(Base_dashboard):
    __tablename__ = "respondedores_centro_enfermedad"

    id = Column(Integer, primary_key=True, index=True)
    enfermedad_cuestionario = Column(String(80), nullable=False)
    tipo_cuestionario = Column(String(50), nullable=False)
    enfermedad = Column(String(50), nullable=False)
    tipo_respondedor = Column(String(50), nullable=False)
    valor = Column(Numeric, nullable=False)
    cliente= Column(String(10), nullable=False)
