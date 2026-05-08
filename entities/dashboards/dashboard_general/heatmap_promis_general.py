from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Heatmap_promis_general(Base_dashboard):
    __tablename__ = "heatmap_promis_general"

    id = Column(Integer, primary_key=True, index=True)
    centro = Column(String(50), nullable=False)
    enfermedad = Column(String(50), nullable=False)
    variable = Column(String(50), nullable=False)
    raw_score = Column(Float, nullable=True)
    tscore = Column(Float, nullable=True)
    se = Column(Float, nullable=True)
    category = Column(String(50), nullable=False)
    color = Column(String(50), nullable=False)
    category_order = Column(Numeric, nullable=False)
    display_value = Column(Float, nullable=True)
    cliente= Column(String(10), nullable=False)
