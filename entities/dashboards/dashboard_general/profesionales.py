from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Profesionales(Base_dashboard):
    __tablename__ = "profesionales"

    profesional = Column(String(80), primary_key=True, index=True)
    valor = Column(String(20), nullable=False)
    cliente= Column(String(10), nullable=False)