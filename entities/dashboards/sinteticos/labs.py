from sqlalchemy import Column, String, Integer, Float, Date, Numeric
from uploads.h2o.resources import Base_dashboard 

class Labs(Base_dashboard):
    __tablename__ = "labs"

    labs_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    origen = Column(String(50), primary_key=True)

    patient_id = Column(String(50),  nullable=False)
    lab_test = Column(String(255), nullable=False)
    lab_result = Column(Numeric, nullable=False)
    lab_unit = Column(String(255), nullable=False)
    lab_date = Column(Date, nullable=False)
    age = Column(Integer, nullable=False)
    gender = Column(String(255), nullable=False)
    type_disease = Column(String(255), nullable=False)
    cliente= Column(String(10),  primary_key=True)


    