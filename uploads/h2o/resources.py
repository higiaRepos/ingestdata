# uploads/h2o/resources.py
from dagster import resource
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL_OMOP = os.getenv('DATABASE_URL_OMOP')
Base_omop = declarative_base()
@resource
def sql_session_resource_omop(_):
    engine = create_engine(DATABASE_URL_OMOP, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

DATABASE_URL_OMOP = os.getenv('DATABASE_URL_OMOP')
Base_omop_athena = declarative_base()
@resource
def sql_session_resource_omop_athena(_):
    engine = create_engine(DATABASE_URL_OMOP, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

DATABASE_URL_DASHBOARD = os.getenv('DATABASE_URL_DASHBOARD')
Base_dashboard = declarative_base()
@resource
def sql_session_resource_dashboard(_):
    engine = create_engine(DATABASE_URL_DASHBOARD, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

DATABASE_URL_CORRELATION = os.getenv('DATABASE_URL_CORRELATION')
Base_correlation = declarative_base()
@resource
def sql_session_resource_correlation(_):
    engine = create_engine(DATABASE_URL_CORRELATION, echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session