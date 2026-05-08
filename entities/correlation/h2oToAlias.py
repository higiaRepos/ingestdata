from sqlalchemy import Column, String, Integer
from uploads.h2o.resources import Base_correlation 

class H2oToAlias(Base_correlation):
    __tablename__ = "h2o_to_alias"

    id_original = Column(String(255), nullable=False,primary_key=True)
    id_alias =Column(String(255), nullable=False,primary_key=True)
    client =Column(String(50), nullable=False)
