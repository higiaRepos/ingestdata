from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Valores_destacados(Base_dashboard):
    __tablename__ = "valores_destacados"

    id = Column(Integer, primary_key=True, index=True)
    metrica = Column(String(50), nullable=False)
    valor = Column(Numeric, nullable=False)
    cliente= Column(String(10), nullable=False)
