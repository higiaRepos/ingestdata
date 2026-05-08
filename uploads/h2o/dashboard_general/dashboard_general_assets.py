from dagster import asset, AssetIn
import pandas as pd

from entities.dashboards.dashboard_general.demografia_enfermedad_edad_genero import Demografia_enfermedad_edad_genero
from entities.dashboards.dashboard_general.localizaciones import Localizaciones
from entities.dashboards.dashboard_general.pacientes_centro_enfermedad import Pacientes_centro_enfermedad
from entities.dashboards.dashboard_general.participacionesmes_centro_enfermedad import Participaciones_mes_centro_enfermedad
from entities.dashboards.dashboard_general.profesionales import Profesionales
from entities.dashboards.dashboard_general.respondedores_centro_enfermedad import Respondedores_centro_enfermedad
from entities.dashboards.dashboard_general.scores_centro_enfermedad import Scores_centro_enfermedad
from entities.dashboards.dashboard_general.valores_destacados import Valores_destacados
from uploads.h2o.resources import Base_dashboard 
from uploads.h2o.correlation_origen import correlation_origen
import os
from dotenv import load_dotenv
load_dotenv()


cliente = os.getenv('CLIENTE', 'vhir')
alias_extension = correlation_origen[cliente]

@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_demografia_enfermedad_edad_genero(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_demografia_enfermedad_edad_genero(context):
    session_dashboard = context.resources.sql_session_dashboard
    engine = session_dashboard.get_bind()
    Base_dashboard.metadata.create_all(engine)
    df=pd.read_json('./uploadsFiles/demografia_enfermedad_edad_genero.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Demografia_enfermedad_edad_genero(

            edad = row['Edad'],
            genero = row['Género'],
            enfermedad = row['Enfermedad'],
            tipo = row['Tipo'],
            valor = row['Valor'],
            cliente="111"
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla demografia_enfermedad_edad_genero.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    
@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_pacientes_centro_enfermedad(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_pacientes_centro_enfermedad(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/pacientes_centro_enfermedad.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Pacientes_centro_enfermedad(

            enfermedad = row['Enfermedad'],
            centro = row['Centro'],
            valor = row['Valor'],
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla pacientes_centro_enfermedad.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    
@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_participaciones_mes_centro_enfermedad(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_participaciones_mes_centro_enfermedad(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/participaciones_mes_centro_enfermedad.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Participaciones_mes_centro_enfermedad(
   
            anyo = row['Año'],
            mes = row['Mes'],
            enfermedad = row['Enfermedad'],
            organizacion = row['Organización'],
            valor = row['Valor'],
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla participacionesmes_centro_enfermedad.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    
@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_respondedores_centro_enfermedad(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_respondedores_centro_enfermedad(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/respondedores_centro_enfermedad.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Respondedores_centro_enfermedad(
            enfermedad_cuestionario=row['Enfermedad']+" - "+row['Tipo de Cuestionario'],
            tipo_cuestionario = row['Tipo de Cuestionario'],
            enfermedad = row['Enfermedad'],
            tipo_respondedor=row['Tipo Respondedor'],
            valor = row['Valor'],
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla respondedores_centro_enfermedad")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    
@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_scores_centro_enfermedad(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_scores_centro_enfermedad(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/scores_centro_enfermedad.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Scores_centro_enfermedad(

            enfermedad = row['Enfermedad'],
            centro=row['Centro'],
            valor = row['Valor'],
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla scores_centro_enfermedad")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    
@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
# def add_valores_destacados(context, addConditions, addMeasurements, addDrugs, addDevice, addProcedures, addObservations):
def add_valores_destacados(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/valores_destacados.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        data = Valores_destacados(

            metrica = row['Métrica'],
            valor = row['Valor'],
        )

        dataList.append(data)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla valores_destacados")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    

@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
def add_profesionales(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/profesionales.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        profesional = Profesionales(

            profesional = row['profesional'],
            valor = row['valor'],
        )

        dataList.append(profesional)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla profesionales")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e
    

@asset(required_resource_keys={"sql_session_dashboard"}, group_name="Dashboard_General")
def add_localizaciones(context):
    session_dashboard = context.resources.sql_session_dashboard

    df=pd.read_json('./uploadsFiles/localizaciones.json')
    dataList=[]
    for index, row in df.iterrows():
        print(row)
        localizacion = Localizaciones(
            Lugar=row['Lugar'],
            Valor = row['Valor'],
            Lat = row['Lat'],
            Lon = row['Lon'],
        )

        dataList.append(localizacion)
    print(dataList)
    session_dashboard.add_all(dataList)
    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(dataList)} registros guardados en la tabla localizaciones")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e