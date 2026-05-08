from dagster import asset, AssetKey
import pandas as pd
from datetime import datetime

from entities.omop.device_exposure import DeviceExposure
from entities.omop.drug_exposure import DrugExposure
from entities.omop.person import Person
from entities.omop.measurement import Measurement
from entities.omop.observation import Observation
from entities.omop.condition_occurrence import ConditionOccurrence
from entities.omop.procedure_occurrence import ProcedureOccurrence
from entities.correlation.h2oToAlias import H2oToAlias
from entities.omop.visit_occurrence import VisitOccurrence
from uploads.h2o.utils.disorders import disorders_dict
from uploads.h2o.utils.encuestas import encuestas
from uploads.h2o.correlation_origen import correlation_origen
from uploads.h2o.athena.concept_assets_athena import addAthena_concepts

from uploads.h2o.resources import Base_omop, Base_correlation

from utils.correlation.h2o_correlation import H2O_MAPPING

import json
import os
from dotenv import load_dotenv
load_dotenv()

from dagster import asset
from sqlalchemy import text
from entities.correlation.h2oToAlias import H2oToAlias
from uploads.h2o.resources import Base_correlation

cliente = os.getenv('CLIENTE', 'vhir')
alias_extension = correlation_origen[cliente]

@asset(required_resource_keys={"sql_session_correlation"}, group_name="Mantenimiento_DB")
def reset_h2o_to_alias_table(context):
    # 1. Obtenemos el motor (engine) desde la sesión para ejecutar DDL
    session = context.resources.sql_session_correlation
    engine = session.get_bind()
    
    try:
        context.log.info("Borrando tabla h2o_to_alias si existe...")
        # Eliminamos la tabla físicamente
        H2oToAlias.__table__.drop(engine, checkfirst=True)
        
        context.log.info("Creando tabla h2o_to_alias con la nueva estructura...")
        # Creamos la tabla basándonos en el nuevo modelo ORM
        H2oToAlias.__table__.create(engine)
        
        context.log.info("Tabla recreada con éxito: [id_original, id_alias, client]")
        
    except Exception as e:
        context.log.error(f"Error al recrear la tabla: {str(e)}")
        raise e
    finally:
        session.close()


@asset(required_resource_keys={"sql_session_correlation"}, group_name="Ingesta_h2o")
def addCorrelation_h2o(context):

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/ALIAS_I_PERSON_ID_VHIR.csv')

    df = df.dropna(subset=['PATIENT_ID', 'ALIAS'])

    os.makedirs('./utils/correlation', exist_ok=True)
    

    mapping_dict = dict(zip(df['PATIENT_ID'], df['ALIAS']))
    with open('./utils/correlation/h2o_correlation.py', 'w') as f:
        f.write("H2O_MAPPING = ")

        f.write(json.dumps(mapping_dict, indent=4, ensure_ascii=False))

    reverse_mapping_dict = dict(zip(df['ALIAS'], df['PATIENT_ID']))
    with open('./utils/correlation/h2o_correlation_reverse.py', 'w') as f:
        f.write("H2O_MAPPING_REVERSE = ")
        f.write(json.dumps(reverse_mapping_dict, indent=4, ensure_ascii=False))

    context.log.info("Ficheros .py generados con formato legible y sin valores NaN.")

    session = context.resources.sql_session_correlation
    try:
        session.query(H2oToAlias).delete()
        
        objetos_insertar = [
            H2oToAlias(
                id_original=row['PATIENT_ID'], 
                id_alias=row['ALIAS'], 
                client="h2o"
            )
            for _, row in df.iterrows()
        ]
        
        session.bulk_save_objects(objetos_insertar)
        session.commit()
        context.log.info(f"Insertados {len(objetos_insertar)} registros en la DB.")
        
    except Exception as e:
        session.rollback()
        context.log.error(f"Error en la ingesta: {str(e)}")
        raise e
    finally:
        session.close()


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def addPersons_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/maestro.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():

        age_val = row['gen_001']
        if pd.notna(age_val) and str(age_val).strip() != "" and str(age_val).lower() != 'nan':
            person_id = str(row['patient_id'])
            alias_id = H2O_MAPPING.get(person_id)

            # Género
            gender = row['gen_002']
            gender_concept_id = 8507 if gender == "M" else 8532 

            # --- MANEJO DE FECHA ---
            val_fecha = row['enrol_date']
            if pd.isna(val_fecha) or str(val_fecha).strip() == "" or str(val_fecha) == 'nan':
                data_asig = "2025-01-01"
            else:
                data_asig = str(val_fecha)
            
            try:
                current_year = int(data_asig.split("-")[0])
            except (ValueError, IndexError, AttributeError):
                current_year = 2025
            age = int(float(age_val))
            year_of_birth = current_year - age
   
            person = Person(
                person_id=alias_id,
                gender_concept_id=gender_concept_id,
                year_of_birth=year_of_birth,
                month_of_birth=None,
                day_of_birth=None,
                birth_datetime=None,
                race_concept_id=0,
                ethnicity_concept_id=0,
                person_source_value=str(person_id),
                gender_source_value=gender,
                care_site_id=alias_extension
            )
            
            session.add(person)

    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def addMaestro_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/maestro.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        if pd.isna(row['patient_id']) or str(row['patient_id']).strip().lower() in ["nan", ""]:
            context.log.warning(f"Fila {index} ignorada: patient_id es nulo o vacío.")
            continue
        person_id = str(row['patient_id'])
        alias_id = H2O_MAPPING.get(person_id)
        val_fecha = row['enrol_date']
        data_asig = "2025-01-01" if pd.isna(val_fecha) or str(val_fecha).strip() in ["", "nan"] else str(val_fecha)
        
        try:

            # --- FUMA ---
            if pd.notna(row['gen_003']) and str(row['gen_003']).strip() != "":
 
                fuma_index = int(row['gen_003'])

                smoke_id = encuestas["smoke"][fuma_index]["id"]
                value_as_string_smoke = encuestas["smoke"][fuma_index]["value"]
                
                smoke = Observation(
                    person_id = alias_id,
                    observation_concept_id = 4275495,
                    observation_date = data_asig,
                    observation_type_concept_id = 32862,
                    value_as_concept_id = smoke_id,
                    value_as_number = fuma_index,
                    value_as_string = value_as_string_smoke,
                    observation_source_value = "Tabaquismo",
                    observation_source_concept_id = 20000019
                )
                session.add(smoke)
            
            # --- ALCOHOL ---
            if pd.notna(row['gen_004']) and str(row['gen_004']).strip() != "":
   
                alcohol_index = int(row['gen_004'])
                alcohol_id = encuestas["alcohol"][alcohol_index]["id"] 
                value_as_string_alc = encuestas["alcohol"][alcohol_index]["value"]
                
                alcohol = Observation(
                    person_id = alias_id,
                    observation_concept_id = 4052946,
                    observation_date = data_asig,
                    observation_type_concept_id = 32862,
                    value_as_concept_id = alcohol_id,
                    value_as_number = alcohol_index,
                    value_as_string = value_as_string_alc,
                    observation_source_value = "Consumo de alcohol",
                    observation_source_concept_id = 20000020
                )
                session.add(alcohol)

            # --- PESO ---
            bmiTrue=0
            if pd.notna(row['gen_005']) and str(row['gen_005']).strip() != "":
                bmiTrue+=1
                peso = float(row['gen_005'])

                peso_measurement = Measurement(
                    person_id=alias_id,
                    measurement_concept_id=4099154,  
                    measurement_date=data_asig,
                    value_as_number=peso,
                    unit_concept_id=9529,  
                    measurement_source_value="Weight",
                    measurement_type_concept_id=32817,
                    measurement_source_concept_id = 20000038
                )
                session.add(peso_measurement)

            # --- ALTURA ---
            if pd.notna(row['gen_006']) and str(row['gen_006']).strip() != "":
                bmiTrue+=1
                altura = float(row['gen_006'])

                altura_measurement = Measurement(
                    person_id=alias_id,
                    measurement_concept_id=4177340,  
                    measurement_date=data_asig,
                    value_as_number=altura,
                    unit_concept_id=8582,  
                    measurement_source_value="Height",
                    measurement_type_concept_id=32817,
                    measurement_source_concept_id = 20000039
                )
                session.add(altura_measurement)

            # --- BMI ---
            if bmiTrue == 2:
                if altura > 0:
                    bmi = peso / ((altura / 100) ** 2)
                else:
                    bmi = 0
                bmi_measurement = Measurement(
                    person_id=alias_id,
                    measurement_concept_id=40490382,  
                    measurement_date=data_asig,
                    value_as_number=bmi,
                    unit_concept_id=9531,  
                    measurement_source_value="BMI",
                    measurement_type_concept_id=32817,
                    measurement_source_concept_id = 20000061
                )
                session.add(bmi_measurement)
            
            # --- ASISTENCIA MEDICA ---
            value_as_string = str(row['gen_007']).strip().upper()

            if value_as_string != "" and value_as_string.lower() not in ["nan", "none"]:
   
                asistencia_id = encuestas["asistencia"][value_as_string]["id"]
                value = encuestas["asistencia"][value_as_string]["value"]

                asistencia_observation= Observation(
                    person_id = alias_id,
                    observation_concept_id = 4021642,
                    observation_date = data_asig,
                    observation_type_concept_id = 32862,
                    value_as_concept_id=asistencia_id,
                    value_as_number= value,
                    value_as_string=value_as_string,
                    observation_source_value= "Asistencia médica",
                    observation_source_concept_id=20000037
                )
                session.add(asistencia_observation)
          
            # --- Primary indication --- 
            value_raw = row['gen_008']
             
            value = str(value_raw).strip() if pd.notna(value_raw) else ""
         

            if value =="Diabetis mellitus tipus 1":
                disorder_concept_id=201254
                value="Diabetes Type 1"
                value_as_number=1
            elif value == "Diabetis mellitus tipus 2":
                disorder_concept_id=201826
                value="Diabetes Type 2"
                value_as_number=2
            else:
                disorder_concept_id= 201820
                value = "Diabetes mellitus"
                value_as_number=3


            primary_condition = ConditionOccurrence(
                person_id = alias_id,
                condition_concept_id = disorder_concept_id,
                condition_start_date = data_asig,
                condition_type_concept_id = 32902,
                condition_source_value =value,
                condition_source_concept_id= 20000062

            )
            session.add(primary_condition)


            primary_observation= Observation(
                person_id = alias_id,
                observation_concept_id = 46234708,
                observation_date = data_asig,
                observation_type_concept_id = 32862,
                value_as_concept_id=disorder_concept_id,
                value_as_number= value_as_number,
                value_as_string=value,
                observation_source_value= value,
                observation_source_concept_id= 20000062,

            )
            session.add(primary_observation)
    
            # --- CONDITIONS ---
            valie= str(row['gen_009'])
            if value != "" and value.lower() not in ["nan", "none"]:
               
                condition = ConditionOccurrence(
                    person_id = alias_id,
                    condition_concept_id = 20000064,
                    condition_start_date =  data_asig,
                    condition_type_concept_id = 32902,
                    condition_source_value = value,
                    condition_source_concept_id= 20000063

                )
                session.add(condition)
            
            # --- OBSERVATION ---
            val_db001 = row.get('db_001')
            if pd.notna(val_db001) and str(val_db001).strip() != "":
                

                observation_value_list = str(val_db001).split(",")
                
                for observation_value in observation_value_list:
                    value=int(observation_value)
                    value_as_string = encuestas["Medicamento"][value]
                    
                    observation = Observation(
                        person_id = alias_id,
                        observation_concept_id = 4301468,
                        observation_date = data_asig,
                        observation_type_concept_id = 32862,
                        value_as_concept_id = 4027003,
                        value_as_number = value,
                        value_as_string = value_as_string,
                        observation_source_value = "Pharmacological treatment modality",
                        observation_source_concept_id = 20000040
                    )   
                    session.add(observation)
            
            val_db002 = row.get('db_002')

            if pd.notna(val_db002) and str(val_db002).strip() != "":
                
                # Convertimos a string por seguridad y separamos por comas
                observation_value_list = str(val_db002).split(",")
                
                for observation_value in observation_value_list:
                    value=int(observation_value)
                    value_as_string = encuestas["Medicamento"][value]
            
                    observation= Observation(
                        person_id = alias_id,
                        observation_concept_id =619861,
                        observation_date = data_asig,
                        observation_type_concept_id = 32862,
                        value_as_concept_id=4321388,
                        value_as_number= value,
                        value_as_string=value_as_string,
                        observation_source_value="Technological treatment modality",
                        observation_source_concept_id=20000041
                    ) 
                    session.add(observation)


            
            val_db020 = row.get('db_020')

            if pd.notna(val_db020) and str(val_db020).strip() != "":
                try:
                    val_db020=val_db020.replace(".", ",")
                    observation_value_list = str(val_db020).split(",")
                    for observation_value in observation_value_list: 
                        value=int(observation_value)
                        value_as_string = encuestas["Complicaciones"][value]     
                        observation= Observation(
                            person_id = alias_id,
                            observation_concept_id =442793,
                            observation_date = data_asig,
                            observation_type_concept_id = 32862,
                            value_as_concept_id=4266812,
                            value_as_number= value,
                            value_as_string=value_as_string,
                            observation_source_value="Complicaciones de diabetes",
                            observation_source_concept_id=20000018
                        )
                        session.add(observation)
                except Exception as e:
                    context.log.error(f"ERROR en Fila {index} | Paciente ID: {person_id} | Detalle: {e}")

                     
            
            
        except Exception as e:
            context.log.error(f"ERROR en Fila {index} | Paciente ID: {person_id} | Detalle: {e}")
            continue 

    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def albumin_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/albumin.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['albumin_date']
        value=row['triglicèrid sèrum']
          
        albuminuria = ConditionOccurrence(
            person_id = alias_id,
            condition_concept_id = 4168705,
            condition_start_date = date,
            condition_type_concept_id = 32902,
            condition_source_value = value,
            condition_source_concept_id= 20000049

        )
        session.add(albuminuria) 
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def blood_pressure_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/blood_pressure.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['blood_pres_date']
        db_015=row['db_015']
        db_014=row['db_014']

        diastolicbp_measurement = Measurement(
            person_id=alias_id,
            measurement_concept_id=4154790,  
            measurement_date=date,
            value_as_number=db_015,
            unit_concept_id=8876,  
            measurement_source_value="Diastolic BP",
            measurement_type_concept_id=32817,
            measurement_source_concept_id = 20000051
        )
        session.add(diastolicbp_measurement)


        systolicbp_measurement = Measurement(
            person_id=alias_id,
            measurement_concept_id=4152194,  
            measurement_date=date,
            value_as_number=db_014,
            unit_concept_id=8876, 
            measurement_source_value="Systolic BP",
            measurement_type_concept_id=32817,
            measurement_source_concept_id = 20000050
        )
        session.add(systolicbp_measurement)
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def cholesterol_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/cholesterol.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['cholest_date']
        db_009=row['db_009']
        db_008=row['db_008']
        db_007=row['db_007']

        if not pd.isna(db_009) and str(db_009).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4101713,  
                measurement_date=date,
                value_as_number=db_009,
                unit_concept_id=8840,  
                measurement_source_value="HDL_mg_dL",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000045
            )
            session.add(measurement)

        if not pd.isna(db_008) and str(db_008).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4012479,  
                measurement_date=date,
                value_as_number=db_008,
                unit_concept_id=8840,  
                measurement_source_value="LDL",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000044
            )
            session.add(measurement)

        if not pd.isna(db_007) and str(db_007).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4008265,  
                measurement_date=date,
                value_as_number=db_007,
                unit_concept_id=8840,  
                measurement_source_value="cholesterol_mg_dL",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000043
            )
            session.add(measurement)
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def creatinine_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/creatinine.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['creat_date']
        db_011=row['db_011']

        systolicbp_measurement = Measurement(
            person_id=alias_id,
            measurement_concept_id=4324383,  
            measurement_date=date,
            value_as_number=db_011,
            unit_concept_id=8840,  
            measurement_source_value="ps_creatinine_mg_dL",
            measurement_type_concept_id=32817,
            measurement_source_concept_id = 20000047
        )
        session.add(systolicbp_measurement)

    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def dds_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop
    # Eliminado nrows=1 para procesar todo el archivo
    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/dds.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date_raw = row['dds17_date']
        if pd.isna(date_raw) or str(date_raw).strip() == "" or str(date_raw).lower() == "nan":
            date = None
        else:
            date = pd.to_datetime(date_raw, dayfirst=True).date()
        
        dds = row['dds17_01']
        encuesta_id = encuestas["DDS"][dds]["id"]
        value_as_string =  encuestas["DDS"][dds]["value"]
        
        response= Observation(
            person_id = alias_id,
            observation_concept_id = 20000001,
            observation_date = date,
            observation_type_concept_id = 32862,
            value_as_concept_id=encuesta_id,
            value_as_number= dds,
            value_as_string=value_as_string,
            observation_source_value= "Sentir que la diabetes consume demasiado de mi energía mental y física cada día.",
            observation_source_concept_id=20000001
        )
        session.add(response)


        dds = row['dds17_02']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000002,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que mi médico no sabe lo suficiente sobre la diabetes y su manejo.",
                observation_source_concept_id=20000002
            )
            session.add(response)

        
        dds = row['dds17_03']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000003,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "No sentirme confiado/a en mi capacidad diaria para manejar la diabetes.",
                observation_source_concept_id=20000003
            )
            session.add(response)


        dds = row['dds17_04']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000004,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentirme enojado, asustado y/o deprimido cuando pienso en vivir con diabetes.",
                observation_source_concept_id=20000004
            )
            session.add(response)


        dds = row['dds17_05']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000005,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que mi médico no me da indicaciones lo suficientemente claras sobre cómo manejar la diabetes.",
                observation_source_concept_id=20000005
            )
            session.add(response)


        dds = row['dds17_06']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000006,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que no me hago pruebas de glucosa lo suficiente.",
                observation_source_concept_id=20000006
            )
            session.add(response)


        dds = row['dds17_07']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000007,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que terminaré con complicaciones graves a largo plazo, sin importar lo que haga.",
                observation_source_concept_id=20000007
            )
            session.add(response)

        dds = row['dds17_08']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000008,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que frecuentemente estoy fallando con mi rutina de diabetes.",
                observation_source_concept_id=20000008
            )
            session.add(response)


        dds = row['dds17_09']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000009,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que amigos o familiares no apoyan lo suficiente mis esfuerzos de autocuidado (por ejemplo, planear actividades que chocan con mi horario o animarme a comer “mal”).",
                observation_source_concept_id=20000009
            )
            session.add(response)


        dds = row['dds17_10']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000010,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que la diabetes controla mi vida.",
                observation_source_concept_id=20000010
            )
            session.add(response)


        dds = row['dds17_11']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000011,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que mi médico no toma en serio mis preocupaciones.",
                observation_source_concept_id=20000011
            )
            session.add(response)


        dds = row['dds17_12']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000012,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que no cumplo lo suficiente con un buen plan alimenticio.",
                observation_source_concept_id=20000012
            )
            session.add(response)


        dds = row['dds17_13']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000013,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que amigos o familia no aprecian lo difícil que puede ser vivir con diabetes.",
                observation_source_concept_id=20000013
            )
            session.add(response)

        dds = row['dds17_14']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000014,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentirme abrumado/a por las exigencias de vivir con diabetes.",
                observation_source_concept_id=20000014
            )
            session.add(response)


        dds = row['dds17_15']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000015,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que no tengo un médico al que pueda ver con regularidad para tratar mi diabetes.",
                observation_source_concept_id=20000015
            )
            session.add(response)


        dds = row['dds17_16']
        if not pd.isna(dds) and str(dds).strip() != '':
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000016,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "No sentir motivación para mantener mi propio manejo de la diabetes.",
                observation_source_concept_id=20000016
            )
            session.add(response)


        dds = row['dds17_17']
        if not pd.isna(dds) and str(dds).strip() != '':
            dds=int(dds)
            encuesta_id = encuestas["DDS"][dds]["id"]
            value_as_string =  encuestas["DDS"][dds]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 20000017,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= dds,
                value_as_string=value_as_string,
                observation_source_value= "Sentir que amigos o familiares no me brindan el apoyo emocional que me gustaría.",
                observation_source_concept_id=20000017
            )
            session.add(response)


    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def didp_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/didp.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date_raw = row['didp_date']
        if pd.isna(date_raw) or str(date_raw).strip() == "" or str(date_raw).lower() == "nan":
            date = None
        else:
            date = pd.to_datetime(date_raw, dayfirst=True).date()
        
        didp = row['didp_1']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764340,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a su salud física?",
                observation_source_concept_id= 20000031,
                qualifier_concept_id=44804048
            )
            session.add(response)

        didp = row['didp_2']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 4072605,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a su situación económica?",
                observation_source_concept_id= 20000032,
                qualifier_concept_id=44804048
            )
            session.add(response)

        didp = row['didp_3']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 4021807,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a sus relaciones con sus familiares, amigos y compañeros?",
                observation_source_concept_id=20000033,
                qualifier_concept_id=44804048
            )
            session.add(response)

        didp = row['didp_4']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 35810768,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a sus actividades de ocio?",
                observation_source_concept_id= 20000034,
                qualifier_concept_id=44804048
            )
            session.add(response)



        didp = row['didp_5']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 4200501,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a su trabajo o estudios?",
                observation_source_concept_id= 20000035,
                qualifier_concept_id=44804048
            )
            session.add(response)

        didp = row['didp_6']
        if didp<8:
            encuesta_id = encuestas["DID"][didp]["id"]
            value_as_string =  encuestas["DID"][didp]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 42870056,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number=didp,
                value_as_string=value_as_string,
                observation_source_value= "¿Cómo afecta la diabetes actualmente a su bienestar emocional?",
                observation_source_concept_id= 20000036,
                qualifier_concept_id=44804048
            )
            session.add(response)
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def hba1c_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/hba1c.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['hba1c_date']
        
        hba1c_val = row['ichom_hba1c']

        if not pd.isna(hba1c_val) and str(hba1c_val).strip() != '':
            HbA1c_measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4184637,  
                measurement_date=date,
                value_as_number=hba1c_val,
                unit_concept_id=8753,
                measurement_source_value="Hemoglobin A1c/Hemoglobin.total in Blood",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000042
            )
            session.add(HbA1c_measurement)

            
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def promis10_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/promis10.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        raw = row['person_id']
        if pd.isna(raw) or str(raw).strip() == '' or str(raw).lower() == 'nan':
            continue
        person_id = str(raw)
        alias_id = H2O_MAPPING.get(person_id)
        date_raw = row['promis-global-10_date']
        if pd.isna(date_raw) or str(date_raw).strip() == "" or str(date_raw).lower() == "nan":
            date = "2025-01-01"
        else:
            date = pd.to_datetime(date_raw, dayfirst=True).date()
        
        promis = row['promis-global-10_1']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764338,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, diría que su salud es:",
                observation_source_concept_id=20000021
            )
            session.add(response)

        promis = row['promis-global-10_2']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764339,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, diría que su calidad de vida es:",
                observation_source_concept_id=20000022
            )
            session.add(response)

        promis = row['promis-global-10_3']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764340,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, ¿cómo clasificaría su salud física?",
                observation_source_concept_id=20000023
            )
            session.add(response)


        promis = row['promis-global-10_4']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764341,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, ¿cómo clasificaría su salud mental, incluidos su estado de ánimo y su capacidad para pensar?",
                observation_source_concept_id=20000024
            )
            session.add(response)



        promis = row['promis-global-10_5']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764342,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, ¿cómo clasificaría su satisfacción con sus actividades sociales y sus relaciones con otras personas?",
                observation_source_concept_id=20000025
            )
            session.add(response)



        promis = row['promis-global-10_6']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10_6"][promis]["id"]
            value_as_string =  encuestas["PROMIS10_6"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764343,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "¿En qué medida puede realizar sus actividades físicas diarias, como caminar, subir escaleras, cargar las compras o mover una silla?",
                observation_source_concept_id=20000026
            )
            session.add(response)


        promis = row['promis-global-10_7rc']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10_7"][promis]["id"]
            value_as_string =  encuestas["PROMIS10_7"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764344,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number=promis,
                value_as_string=value_as_string,
                observation_source_value= "En los últimos 7 días: En promedio, ¿cómocalificaría su dolor?",
                observation_source_concept_id=20000027
            )
            session.add(response)


        promis = row['promis-global-10_8r']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10_8"][promis]["id"]
            value_as_string =  encuestas["PROMIS10_8"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764345,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En los últimos 7 días: En promedio, ¿cómo calificaría su cansancio?",
                observation_source_concept_id=20000028
            )
            session.add(response)


        promis = row['promis-global-10_9r']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764346,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En general, califique en qué medida puede realizar sus actividades sociales y funciones habituales. (Esto comprende las actividades en casa, en el trabajo y en el área donde reside, así como sus responsabilidades como padre o madre, hijo/a, cónyuge, empleado/a, amigo/a, etc.)",
                observation_source_concept_id=20000029
            )
            session.add(response)


        promis = row['promis-global-10_10r']
        if not pd.isna(promis) and str(promis).strip() != '' and promis > 0:
            promis=int(promis)
            encuesta_id = encuestas["PROMIS10_10"][promis]["id"]
            value_as_string =  encuestas["PROMIS10_10"][promis]["value"]
            
            response= Observation(
                person_id = alias_id,
                observation_concept_id = 40764347,
                observation_date = date,
                observation_type_concept_id = 32862,
                value_as_concept_id=encuesta_id,
                value_as_number= promis,
                value_as_string=value_as_string,
                observation_source_value= "En los últimos 7 días: ¿Con qué frecuencia le han afectado problemas emocionales como sentir ansiedad, depresión o irritabilidad?",
                observation_source_concept_id=20000030
            )
            session.add(response)
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def triglycerides_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/triglycerides.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['trig_date']
        
        value = row['db_010']
        if not pd.isna(value) and str(value).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4032789,  
                measurement_date=date,
                value_as_number=value,
                unit_concept_id=8840,  
                measurement_source_value="triglycerides_mg_dL",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000046
            )
            session.add(measurement)
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"


@asset(required_resource_keys={"sql_session_omop"}, group_name="Ingesta_h2o")
def DATOS_SENSORES_h2o(context, addCorrelation_h2o):
    session = context.resources.sql_session_omop

    df = pd.read_csv('./uploadsFiles/Datos_H2O/DB/DATOS_SENSORES.csv')
    df.columns = df.columns.str.lower()
        
    for index, row in df.iterrows():
        person_id = str(row['person_id'])
        alias_id = H2O_MAPPING.get(person_id)
        date = row['visita_descarga_basal']
        value = row['glucpromig_basal']
        if not pd.isna(value) and str(value).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=37398152,  
                measurement_date=date,
                value_as_number=value,
                unit_concept_id=8840,  
                measurement_source_value="Glucosa promedio",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000057
            )
            session.add(measurement)

        value = row['tir_basal']
        if not pd.isna(value) and str(value).strip() != '':
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=1617716,  
                measurement_date=date,
                value_as_number=value,
                unit_concept_id=8554,  
                measurement_source_value="TIR",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000059
            )
            session.add(measurement)

        value1=row['tbaix_basal']
        value2=row['tmoltbaix_basal']
        if not pd.isna(value1) and str(value1).strip() != '' and not pd.isna(value2) and str(value2).strip() != '':
            value = float(value1)+float(value2)
            measurement = Measurement(
                person_id=alias_id,
                measurement_concept_id=4029422,  
                measurement_date=date,
                value_as_number=value,
                unit_concept_id=8554,  
                measurement_source_value="TIH/TBR",
                measurement_type_concept_id=32817,
                measurement_source_concept_id = 20000060
            )
            session.add(measurement)
            
    try:
        session.commit()
        return df
    except Exception as e:
        context.log.error(f"Error al hacer commit: {e}")
        session.rollback()
        return "Falló el commit"