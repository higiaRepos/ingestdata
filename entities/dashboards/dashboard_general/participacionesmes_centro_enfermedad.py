from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Participaciones_mes_centro_enfermedad(Base_dashboard):
    __tablename__ = "participaciones_mes_centro_enfermedad"

    id = Column(Integer, primary_key=True, index=True)
    anyo = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    enfermedad = Column(String(50), nullable=False)
    organizacion = Column(String(50), nullable=False)
    valor = Column(Numeric, nullable=False)
    cliente= Column(String(10), nullable=False)
