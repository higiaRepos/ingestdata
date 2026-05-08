from dagster import Definitions, with_resources

from uploads.h2o.resources import (
    sql_session_resource_omop,
    sql_session_resource_dashboard,
    sql_session_resource_correlation,
    sql_session_resource_omop_athena
)

from uploads.h2o.athena.concept_assets_athena import (
    addConcept_Ancestors, addConcept_Classes, addConcept_Relationship,addConcepSynonym,
    addAthena_concepts,addDomains,addDrug_strength,addRelationship,addVocavulary
)

from uploads.h2o.dashboard_general.dashboard_general_assets import (
    add_demografia_enfermedad_edad_genero, add_pacientes_centro_enfermedad, add_participaciones_mes_centro_enfermedad,
    add_respondedores_centro_enfermedad, add_scores_centro_enfermedad, add_valores_destacados, add_profesionales, add_localizaciones
)

from uploads.h2o.h2o_real.data_assets_omop import(
    addCorrelation_h2o, addPersons_h2o,addMaestro_h2o, albumin_h2o, blood_pressure_h2o, cholesterol_h2o,creatinine_h2o, dds_h2o, didp_h2o, hba1c_h2o, promis10_h2o, triglycerides_h2o, DATOS_SENSORES_h2o
)

from uploads.h2o.h2o_real.data_assets_dashboards import (
    addDemografia_h2o, addLabs_h2o, addQuestions_h2o
)


assets_with_resources = with_resources(
    [
        addConcept_Ancestors, addConcept_Classes, addConcept_Relationship,addConcepSynonym,
        addAthena_concepts,addDomains,addDrug_strength,addRelationship,addVocavulary,

        add_demografia_enfermedad_edad_genero,add_pacientes_centro_enfermedad,add_participaciones_mes_centro_enfermedad,
        add_respondedores_centro_enfermedad, add_scores_centro_enfermedad, add_valores_destacados, add_profesionales, add_localizaciones,
    
        addCorrelation_h2o, addPersons_h2o, addMaestro_h2o, albumin_h2o, blood_pressure_h2o, cholesterol_h2o,creatinine_h2o, dds_h2o, didp_h2o, hba1c_h2o, promis10_h2o, triglycerides_h2o, DATOS_SENSORES_h2o,
        addDemografia_h2o, addLabs_h2o, addQuestions_h2o
    ],
    resource_defs={
        "sql_session_omop": sql_session_resource_omop,
        "sql_session_dashboard": sql_session_resource_dashboard,
        "sql_session_correlation": sql_session_resource_correlation,
        "sql_session_omop_athena":sql_session_resource_omop_athena
    },
)

defs = Definitions(
    assets=assets_with_resources,
)
