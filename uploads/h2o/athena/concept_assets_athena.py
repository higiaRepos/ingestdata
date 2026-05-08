from dagster import asset, AssetKey
import pandas as pd
import math
import datetime
import sys
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, func, distinct
from uploads.h2o.resources import Base_omop_athena
import json
from pathlib import Path

from entities.omop_athena.domain import Domain
from entities.omop_athena.concept_class import ConceptClass
from entities.omop_athena.vocabulary import Vocabulary
from entities.omop_athena.concept import Concept
from entities.omop_athena.concept_ancestor import ConceptAncestor
from entities.omop_athena.concept_relationship import ConceptRelationship
from entities.omop_athena.concept_synonym import ConceptSynonym
from entities.omop_athena.drug_strength import DrugStrength
from entities.omop_athena.relationship import Relationship

import pandas as pd
import datetime

import pandas as pd
import datetime
import math


def limpiar_dataframe(df):
    # Convertir NaN a None para que PostgreSQL lo interprete como NULL
    df = df.where(pd.notnull(df), None)

    # Truncar columnas con límites de caracteres
    if 'invalid_reason' in df.columns:
        df['invalid_reason'] = df['invalid_reason'].astype(str).str[:1]
        # Convertir strings 'None' a None real
        df['invalid_reason'] = df['invalid_reason'].replace({'None': None})

    # Ejemplo: si relationship_id tuviera límite de 20 caracteres
    if 'relationship_id' in df.columns:
        df['relationship_id'] = df['relationship_id'].astype(str).str[:20]
        df['relationship_id'] = df['relationship_id'].replace({'None': None})

    # Eliminar filas donde campos obligatorios estén vacíos
    columnas_obligatorias = ['concept_id_1', 'concept_id_2', 'relationship_id']
    df = df.dropna(subset=columnas_obligatorias)

    return df

from sqlalchemy import inspect

@asset(required_resource_keys={"sql_session_omop_athena"},group_name="Ingesta_omop_ddbb_athena")
def createAllTables(context):

    engine = context.resources.sql_session_omop_athena.get_bind()
    Base_omop_athena.metadata.create_all(engine)
    context.log.info("✅ Tablas omop_athena creadas correctamente.")
    print(Base_omop_athena.metadata.tables.keys())

@asset(required_resource_keys={"sql_session_omop_athena"},group_name="Ingesta_omop_ddbb_athena")
def dropAllTables(context):

    engine = context.resources.sql_session_omop_athena.get_bind()
    Base_omop_athena.metadata.drop_all(engine)
    context.log.info("✅ Tablas omop_athena Borradas correctamente.")
    print(Base_omop_athena.metadata.tables.keys())


@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addConcept_Ancestors(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/CONCEPT_ANCESTOR.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)


    total_insertados = 0

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:

            registros = chunk.to_dict(orient="records")

            session.bulk_insert_mappings(ConceptAncestor, registros)
            session.commit()

            total_insertados += len(registros)

            msg = f"Chunk {idx}/{total_chunks} subido ({len(registros)} filas)"
            sys.stdout.write("\r" + msg)
            sys.stdout.flush()

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")

    return f"Inserción completada. Total filas insertadas: {total_insertados}"

@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addConcept_Classes(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/CONCEPT_CLASS.csv'

    # Contar filas del CSV
    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)

    total_insertados_reales = 0

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:
            registros = chunk.to_dict(orient="records")

            stmt = insert(ConceptClass).values(registros)
            stmt = stmt.on_conflict_do_nothing()

            result = session.execute(stmt)
            session.commit()

            insertados_chunk = result.rowcount or 0
            total_insertados_reales += insertados_chunk

            msg = (
                f"Chunk {idx}/{total_chunks} -> "
                f"insertados {insertados_chunk}, "
                f"saltados {len(registros) - insertados_chunk}"
            )

            sys.stdout.write("\r" + msg)
            sys.stdout.flush()

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")

    return (
        f"Inserción completada. "
        f"Total filas insertadas realmente: {total_insertados_reales}"
    )


@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena") # 373 blques de 100000
def addConcept_Relationship(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/CONCEPT_RELATIONSHIP.csv'
    chunk_size = 100000

    chunk_iter = pd.read_csv(file_path, sep="\t", dtype=str, chunksize=chunk_size)

    for idx, chunk in enumerate(chunk_iter, start=1):
        try:
            # Limpiar todo el chunk de una vez
            chunk_limpio = limpiar_dataframe(chunk)

            # Convertir a lista de diccionarios
            registros_limpios = chunk_limpio.to_dict(orient="records")

            # Inserción rápida en la DB
            session.bulk_insert_mappings(ConceptRelationship, registros_limpios)
            session.commit()

            context.log.info(f"Chunk {idx} insertado: {len(registros_limpios)} registros")

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")

            
@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addConcepSynonym(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/CONCEPT_SYNONYM.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)

    total_insertados = 0

    for idx, chunk in enumerate(chunk_iter, start=1):
        try:
            
            registros = chunk.to_dict(orient="records")

            stmt = insert(ConceptSynonym).values(registros)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["concept_id", "concept_synonym_name", "language_concept_id"]
            )

            result = session.execute(stmt)
            session.commit()

            insertados_chunk = result.rowcount
            total_insertados += insertados_chunk

            context.log.info(
                f"Chunk {idx}/{total_chunks} procesado "
                f"({len(registros)} filas leídas, {insertados_chunk} insertadas)"
            )

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")

    return f"Inserción completada. Total filas insertadas realmente: {total_insertados}"

@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addAthena_concepts(
    context, addConcept_Ancestors, addConcept_Relationship, addConcept_Classes,
    addConcepSynonym, addDomains,  addRelationship, addVocavulary, addDrug_strength
):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/CONCEPT.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)

    total_insertados = 0
    required_columns = ["concept_name", "domain_id", "vocabulary_id", "concept_class_id", "concept_code"]

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:
            chunk = chunk.dropna(subset=required_columns)
            chunk = chunk.astype(object).where(pd.notna(chunk), None)
            chunk["valid_start_date"] = pd.to_datetime(chunk["valid_start_date"], errors="coerce")
            chunk["valid_end_date"] = pd.to_datetime(chunk["valid_end_date"], errors="coerce")

            chunk.to_sql(
                "concept",
                session.bind,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=chunk_size
            )
            session.commit()

            total_insertados += len(chunk)

            # Mensaje de progreso
            context.log.info(
                f"✔ Chunk {idx} / {total_chunks} procesado ({len(chunk)} filas)."
            )

        except Exception as e:
            session.rollback()
            context.log.error(f"ERROR en chunk {idx}: {e}")
            raise e  

    context.log.info(f"FINALIZADO: total registros insertados → {total_insertados}")

    return total_insertados


@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addDomains(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/DOMAIN.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)


    total_insertados = 0

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:

            registros = chunk.to_dict(orient="records")

            session.bulk_insert_mappings(Domain, registros)
            session.commit()

            total_insertados += len(registros)

            msg = f"Chunk {idx}/{total_chunks} subido ({len(registros)} filas)"
            sys.stdout.write("\r" + msg)
            sys.stdout.flush()

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")
            input("Presiona intro para continuar...")

 
    return f"Inserción completada. Total filas insertadas: {total_insertados}"


@asset(
    required_resource_keys={"sql_session_omop_athena"},
    group_name="Ingesta_omop_ddbb_athena"
)
def addDrug_strength(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/DRUG_STRENGTH.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)

    total_insertados = 0

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:
            chunk = chunk.astype(object).where(pd.notna(chunk), None)
            chunk["valid_start_date"] = pd.to_datetime(chunk["valid_start_date"], errors="coerce")
            chunk["valid_end_date"] = pd.to_datetime(chunk["valid_end_date"], errors="coerce")

            chunk.to_sql(
                "drug_strength",
                session.bind,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=chunk_size
            )
            session.commit()

            total_insertados += len(chunk)

            # Mensaje de progreso
            context.log.info(
                f"✔ Chunk {idx} / {total_chunks} procesado ({len(chunk)} filas)."
            )

        except Exception as e:
            session.rollback()
            context.log.error(f"ERROR en chunk {idx}: {e}")
            raise e  

    context.log.info(f"FINALIZADO: total registros insertados → {total_insertados}")

    return total_insertados

@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addRelationship(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/RELATIONSHIP.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)


    total_insertados = 0


    for idx, chunk in enumerate(chunk_iter, start=1):

        try:

            registros = chunk.to_dict(orient="records")

            session.bulk_insert_mappings(Relationship, registros)
            session.commit()

            total_insertados += len(registros)

            msg = f"Chunk {idx}/{total_chunks} subido ({len(registros)} filas)"
            sys.stdout.write("\r" + msg)
            sys.stdout.flush()

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")
            input("Presiona intro para continuar...")

 
    return f"Inserción completada. Total filas insertadas: {total_insertados}"

@asset(required_resource_keys={"sql_session_omop_athena"}, group_name="Ingesta_omop_ddbb_athena")
def addVocavulary(context):

    session = context.resources.sql_session_omop_athena
    file_path = './uploadsFiles/VOCABULARY.csv'

    with open(file_path) as f:
        total_filas = sum(1 for _ in f) - 1 
    
    chunk_size = 100000
    total_chunks = math.ceil(total_filas / chunk_size)

    context.log.info(f"Total filas: {total_filas}, total chunks: {total_chunks}, de {chunk_size} registros")

    chunk_iter = pd.read_csv(file_path, sep="\t", chunksize=chunk_size)


    total_insertados = 0

    for idx, chunk in enumerate(chunk_iter, start=1):

        try:

            registros = chunk.to_dict(orient="records")

            session.bulk_insert_mappings(Vocabulary, registros)
            session.commit()

            total_insertados += len(registros)

            msg = f"Chunk {idx}/{total_chunks} subido ({len(registros)} filas)"
            sys.stdout.write("\r" + msg)
            sys.stdout.flush()

        except Exception as e:
            session.rollback()
            context.log.error(f"Error en chunk {idx}: {e}")
            input("Presiona intro para continuar...")

 
    return f"Inserción completada. Total filas insertadas: {total_insertados}"