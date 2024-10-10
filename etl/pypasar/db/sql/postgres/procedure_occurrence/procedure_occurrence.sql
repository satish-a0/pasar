-- *******************************************************************
-- NAME: procedure_occurrence.sql
-- DESC: Final table - procedure_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-10-10  1.00           Initial create
-- *******************************************************************

    INSERT INTO {OMOP_SCHEMA}.procedure_occurrence
    (
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_end_date,
        procedure_end_datetime,
        procedure_type_concept_id,
        modifier_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        procedure_source_value,
        procedure_source_concept_id,
        modifier_source_value
    )
    SELECT
        procedure_occurence_id    AS procedure_occurrence_id,
        person_id    AS person_id,
        procedure_concept_id    AS procedure_concept_id,
        procedure_date    AS procedure_date,
        procedure_datetime    AS procedure_datetime,
        procedure_end_date    AS procedure_end_date,
        procedure_end_datetime    AS procedure_end_datetime,
        procedure_type_concept_id    AS procedure_type_concept_id,
        modifier_concept_id    AS modifier_concept_id,
        quantity    AS quantity,
        provider_id    AS provider_id,
        visit_occurrence_id    AS visit_occurrence_id,
        visit_detail_id    AS visit_detail_id,
        procedure_source_value    AS procedure_source_value,
        procedure_source_concept_id    AS procedure_source_concept_id,
        modifier_source_value    AS modifier_source_value
    FROM {OMOP_SCHEMA}.stg__procedure_occurrence;