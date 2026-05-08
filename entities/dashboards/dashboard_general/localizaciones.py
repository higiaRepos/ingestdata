from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Localizaciones(Base_dashboard):
    __tablename__ = "localizaciones"

    Lugar = Column(String(50), primary_key=True, index=True)
    Valor = Column(Integer, nullable=False)
    Lat = Column(Numeric, nullable=False)
    Lon = Column(Numeric, nullable=False)
    cliente= Column(String(10), nullable=False)