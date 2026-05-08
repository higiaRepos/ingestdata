from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Demografia_enfermedad_edad_genero(Base_dashboard):
    __tablename__ = "demografia_enfermedad_edad_genero"

    id = Column(Integer, primary_key=True, index=True)
    edad = Column(String(10), nullable=False)
    genero = Column(String(50), nullable=False)
    enfermedad = Column(String(50), nullable=False)
    tipo = Column(String(50), nullable=False)
    valor = Column(Numeric, nullable=False)
    cliente= Column(String(50), nullable=False)
