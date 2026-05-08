from dagster import asset
import pandas as pd

from sqlalchemy import text, func, distinct,inspect
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import aliased

from uploads.h2o.resources import Base_dashboard 
from uploads.h2o.correlation_origen import correlation_origen
import os
from dotenv import load_dotenv
load_dotenv()

from entities.omop.concept import Concept
from entities.omop.concept_class import ConceptClass
from entities.omop.vocabulary import Vocabulary
from entities.omop.person import Person
from entities.omop.observation import Observation
from entities.omop.measurement import Measurement


from entities.dashboards.test import Test
from entities.dashboards.sinteticos.demografia import Demografia
from entities.dashboards.sinteticos.labs import Labs
from entities.dashboards.sinteticos.cuestionario import Cuestionario
from entities.dashboards.sinteticos.tratamiento import Tratamiento

cliente = os.getenv('CLIENTE', 'vhir')
alias_cliente = correlation_origen[cliente]
origen = os.getenv('ORIGEN', 'diabetes')

def getObservationGeneralType(typeObObservation:str):
    if "DB" in typeObObservation:
        return "DB"
    elif "DID" in typeObObservation:
        return "DIDP"
    elif "GEN" in typeObObservation:
        return "GEN"
    elif "PROMIS" in typeObObservation:
        return "PROMIS-GLOBAL"
    elif "DDS" in typeObObservation:
        return "DDS17"
    else:
        return ""
    
def get_age_group(age):
    if 18 <= age <= 29:
        return "18-29"
    elif 30 < age <= 65:
        return "30-65"
    else:
        return ">65"

def get_bmi_group(bmi):
    if  bmi <= 18.5:
        return "Underweight (<18.5)"
    elif 18.5 < bmi <= 25:
        return "Normal (18.5 - 25)"
    elif 25 < bmi <= 30:
        return "Overweight (25 - 30)"
    else:
        return "Obese (> 30)"


dicionario_cuestionario_conceptos = {
    "DDS17_01": "Agotamiento general",
    "DDS17_02": "Desconocimiento del profesional",
    "DDS17_03": "Salud mental",
    "DDS17_04": "Falta de recomendaciones",
    "DDS17_05": "Análisis sangre",
    "DDS17_06": "Rutina diabetes",
    "DDS17_07": "Relaciones interpersonales",
    "DDS17_08": "Control de vida",
    "DDS17_09": "Preocupaciones profesionales",
    "DDS17_10": "Autoconfianza manejo enfermedad",
    "DDS17_11": "Complicaciones largo plazo",
    "DDS17_12": "Falta mantenimiento régimen",
    "DDS17_13": "Relaciones interpersonales",
    "DDS17_14": "Abrumado",
    "DDS17_15": "Preocupaciones profesionales",
    "DDS17_16": "Falta de motivación",
    "DDS17_17": "Apoyo emocional circulo",
    "DIDP_1": "Salud física",
    "DIDP_2": "Situación económica",
    "DIDP_3": "Relaciones interpersonales",
    "DIDP_4": "Actividades de ocio",
    "DIDP_5": "Trabajo/estudios",
    "DIDP_6": "Bienestar emocional"
}

@asset(required_resource_keys={"sql_session_omop", "sql_session_dashboard"}, group_name="Dashboard_diabetes_h2o")
def addDemografia_h2o(context, addPersons_h2o, addMaestro_h2o, albumin_h2o, blood_pressure_h2o, cholesterol_h2o,creatinine_h2o, dds_h2o, didp_h2o, hba1c_h2o, promis10_h2o, triglycerides_h2o, DATOS_SENSORES_h2o):
    session_omop = context.resources.sql_session_omop
    session_dashboard = context.resources.sql_session_dashboard

    context.log.info("🚀 Iniciando extracción de conceptos desde OMOP...")

    GenderConcept = aliased(Concept)
    PrimaryObservation = aliased(Observation)
    ComplicationObservation= aliased(Observation)
    MedicamentosFarmacologicosObservation= aliased(Observation)
    MedicamentosProceduralesObservation= aliased(Observation)
    PesoMeasurement= aliased(Measurement)
    AlturaMeasurement= aliased(Measurement)
    FumaObservation= aliased(Observation)
    AlcoholObservation= aliased(Observation)
    results = (
        session_omop.query(
            Person.person_id,
            Person.year_of_birth,
            GenderConcept.concept_name.label("gender"),
            PrimaryObservation.value_as_string.label("type_disease"),
            PrimaryObservation.observation_date.label("date"),
            PesoMeasurement.value_as_number.label("peso"),
            AlturaMeasurement.value_as_number.label("altura"),
            FumaObservation.value_as_string.label("fuma"),
            AlcoholObservation.value_as_string.label("alcohol"),
            AlturaMeasurement.value_as_number.label("altura"),
            func.array_agg(distinct(ComplicationObservation.value_as_string)).label("comorbidities"),
            func.array_agg(distinct(MedicamentosFarmacologicosObservation.value_as_string)).label("DB_001"),
            func.array_agg(distinct(MedicamentosProceduralesObservation.value_as_string)).label("DB_002")
        )
        .join(GenderConcept, GenderConcept.concept_id == Person.gender_concept_id)
        .join(PrimaryObservation, (PrimaryObservation.person_id == Person.person_id) & (PrimaryObservation.observation_concept_id==46234708))
        .join(PesoMeasurement, (PesoMeasurement.person_id == Person.person_id) & (PesoMeasurement.measurement_concept_id==4099154))
        .join(AlturaMeasurement, (AlturaMeasurement.person_id == Person.person_id) & (AlturaMeasurement.measurement_concept_id==4177340))
        .join(FumaObservation, (FumaObservation.person_id == Person.person_id) & (FumaObservation.observation_concept_id==4275495))
        .join(AlcoholObservation, (AlcoholObservation.person_id == Person.person_id) & (AlcoholObservation.observation_concept_id==4052946))
        .outerjoin(ComplicationObservation, (ComplicationObservation.person_id == Person.person_id) & (ComplicationObservation.observation_concept_id == 442793))
        .outerjoin(MedicamentosFarmacologicosObservation, (MedicamentosFarmacologicosObservation.person_id == Person.person_id) & (MedicamentosFarmacologicosObservation.observation_concept_id == 4301468))
        .outerjoin(MedicamentosProceduralesObservation, (MedicamentosProceduralesObservation.person_id == Person.person_id) & (MedicamentosProceduralesObservation.observation_concept_id == 619861))
        .filter(Person.care_site_id == alias_cliente) 
        .group_by(Person.person_id, Person.year_of_birth, GenderConcept.concept_name, PrimaryObservation.value_as_string, PrimaryObservation.observation_date, PesoMeasurement.value_as_number,AlturaMeasurement.value_as_number,FumaObservation.value_as_string,AlcoholObservation.value_as_string)
        .all()
    )

    demografias = []
    for r in results:
        print(r)
        age = (r.date.year - r.year_of_birth)
        altura= r.altura/100
        bmi = r.peso / (altura ** 2)
        
        demografia = Demografia(
            patient_id=r.person_id,
            gender=r.gender,
            age=age,
            type_disease=r.type_disease or "Unknown",
            comorbidities= r.comorbidities or [],
            num_comorbidities=len(r.comorbidities) or 0,
            DB_001=r.DB_001,
            DB_002=r.DB_002,
            ENROL_DATE= r.date,
            altura=altura,
            peso= r.peso,
            fuma=r.fuma,
            alcohol=r.alcohol,
            age_group=get_age_group(age),
            bmi=bmi,
            bmi_cat=get_bmi_group(bmi),
            origen=origen,
            cliente=alias_cliente
        )

        demografias.append(demografia)
    session_dashboard.add_all(demografias)

    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(demografias)} registros guardados en la tabla demografia.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e



@asset(required_resource_keys={"sql_session_omop", "sql_session_dashboard"}, group_name="Dashboard_diabetes_h2o")
def addLabs_h2o(context, addPersons_h2o, addMaestro_h2o, albumin_h2o, blood_pressure_h2o, cholesterol_h2o,creatinine_h2o, dds_h2o, didp_h2o, hba1c_h2o, promis10_h2o, triglycerides_h2o, DATOS_SENSORES_h2o):
    session_omop = context.resources.sql_session_omop
    session_dashboard = context.resources.sql_session_dashboard

    context.log.info("🚀 Iniciando extracción de conceptos desde OMOP...")


    UnitConcept = aliased(Concept)
    GenderConcept = aliased(Concept)
    PrimaryObservation = aliased(Observation)
  
    results = (
        session_omop.query(
            Person.person_id,
            Person.year_of_birth,
            Measurement.measurement_source_value.label("label"),
            Measurement.value_as_number.label("result"),
            UnitConcept.concept_code.label("units"),
            Measurement.measurement_date.label("date"),
            GenderConcept.concept_name.label("gender"),
            PrimaryObservation.value_as_string.label("type_disease"),
            
        )
        .select_from(Person)
        .where(Person.care_site_id == alias_cliente)
        .join(Measurement, Measurement.person_id == Person.person_id)
        .join(UnitConcept, UnitConcept.concept_id == Measurement.unit_concept_id)
        .join(GenderConcept, GenderConcept.concept_id == Person.gender_concept_id)
        .join(PrimaryObservation, (PrimaryObservation.person_id == Person.person_id) & (PrimaryObservation.observation_concept_id==46234708))
        
        .all()
    )

    labs=[]
    for r in results:
        age = (r.date.year - r.year_of_birth)
        lab = Labs(
            patient_id=r.person_id,
            age=age,
            gender=r.gender,
            lab_test=r.label, 
            lab_result=r.result,
            lab_unit=r.units,
            lab_date=r.date,
            type_disease=r.type_disease,
            origen=origen,
            cliente=alias_cliente

        )

        labs.append(lab)
    session_dashboard.add_all(labs)

    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(labs)} registros guardados en la tabla demografia.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e




@asset(required_resource_keys={"sql_session_omop", "sql_session_dashboard"}, group_name="Dashboard_diabetes_h2o")
def addQuestions_h2o(context, addPersons_h2o, addMaestro_h2o, albumin_h2o, blood_pressure_h2o, cholesterol_h2o,creatinine_h2o, dds_h2o, didp_h2o, hba1c_h2o, promis10_h2o, triglycerides_h2o, DATOS_SENSORES_h2o):
    session_omop = context.resources.sql_session_omop
    session_dashboard = context.resources.sql_session_dashboard

    context.log.info("🚀 Iniciando extracción de conceptos desde OMOP...")

    GenderConcept = aliased(Concept)
    TypeConcept = aliased(Concept)
    PrimaryObservation = aliased(Observation)
  
    results = (
        session_omop.query(
            Person.person_id,
            Person.year_of_birth,
            GenderConcept.concept_name.label("gender"),
            Observation.observation_source_value.label("pregunta"),
            Observation.value_as_number.label("respuesta_numerica"),
            Observation.value_as_string.label("respuesta"),
            Observation.observation_date.label("date"),
            TypeConcept.concept_name.label("type"),
            PrimaryObservation.value_as_string.label("type_disease"),
            
        )
        .select_from(Person)
        .where(Person.care_site_id == alias_cliente)
        .join(Observation, Observation.person_id == Person.person_id)
        .join(GenderConcept, GenderConcept.concept_id == Person.gender_concept_id)
        .join(TypeConcept, TypeConcept.concept_id == Observation.observation_source_concept_id)
        .join(PrimaryObservation, (PrimaryObservation.person_id == Person.person_id) & (PrimaryObservation.observation_concept_id==46234708))
        
        .all()
    )
 
    cuestionarios=[]
    for r in results:
        age = (r.date.year - r.year_of_birth)
        type_info_value = dicionario_cuestionario_conceptos.get(r.type, "")
        if type_info_value:
            type_info_full = f"{r.type}({type_info_value})"
        else:
            type_info_full = ""
        cuestionario = Cuestionario(
            patient_id=r.person_id,
            type=r.type,
            cuestion=r.pregunta, 
            answer=r.respuesta,
            numeric_answer=r.respuesta_numerica,
            cuestion_date=r.date,
            age=age,
            gender=r.gender,
            type_disease=r.type_disease,
            general_type=getObservationGeneralType(r.type),
            type_info=type_info_full,
            origen=origen,
            cliente=alias_cliente
        )

        cuestionarios.append(cuestionario)
    session_dashboard.add_all(cuestionarios)

    try:
        session_dashboard.commit()
        context.log.info(f"💾 {len(cuestionarios)} registros guardados en la tabla demografia.")
    except Exception as e:
        session_dashboard.rollback()
        context.log.error(f"❌ Error al hacer commit: {e}")
        raise e

