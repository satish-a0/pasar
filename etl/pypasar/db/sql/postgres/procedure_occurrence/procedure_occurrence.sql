-- *******************************************************************
-- NAME: procedure_occurrence.sql
-- DESC: Load process - procedure_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-08-19  1.00           Initial create
--
-- *******************************************************************

-- -------------------------------------------------------------------
-- Load Start
-- -------------------------------------------------------------------

/*!@insert
   @table: #ETL_SCHEMA_NAME#.procedure_occurrence
   @source_table: #SR_SCHEMA_NAME#.sr_table_name
   @summary: Take all records from source table to standard format
!*/
    INSERT INTO #ETL_SCHEMA_NAME#.procedure_occurrence
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
        NULL    AS procedure_occurrence_id,
        NULL    AS person_id,
        NULL    AS procedure_concept_id,
        NULL    AS procedure_date,
        NULL    AS procedure_datetime,
        NULL    AS procedure_end_date,
        NULL    AS procedure_end_datetime,
        NULL    AS procedure_type_concept_id,
        NULL    AS modifier_concept_id,
        NULL    AS quantity,
        NULL    AS provider_id,
        NULL    AS visit_occurrence_id,
        NULL    AS visit_detail_id,
        NULL    AS procedure_source_value,
        NULL    AS procedure_source_concept_id,
        NULL    AS modifier_source_value
    FROM #SR_SCHEMA_NAME#.sr_table_name
    ;

    COMMIT;

-- -------------------------------------------------------------------
-- Load End
-- -------------------------------------------------------------------