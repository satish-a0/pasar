-- *******************************************************************
-- NAME: stg__procedure_occurrence.sql
-- DESC: Create the staging view - procedure_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-10-10  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the procedure_occurrence table, assigning a unique procedure_occurrence_id
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__procedure_occurrence AS 
    WITH intra_op__operation AS (
        SELECT ROW_NUMBER() OVER (ORDER BY procedure_date asc) AS procedure_occurence_id, temp.*
        FROM (
            SELECT DISTINCT
                person.person_id,
                0 AS procedure_concept_id, -- Need concept id mapping
                operation.operation_startdate AS procedure_date,
                CONCAT(operation.operation_startdate, ' ', operation.operation_starttime) ::timestamp AS procedure_datetime,
                operation.operation_enddate AS procedure_end_date,
                CONCAT(operation.operation_enddate, ' ', operation.operation_endtime) ::timestamp AS procedure_end_datetime,
                32879 AS procedure_type_concept_id,
                0 AS modifier_concept_id,
                0 AS quantity,
                provider.provider_id AS provider_id,
                operation.session_id AS visit_occurrence_id,
                0 AS visit_detail_id,
                operation.procedure_code AS procedure_source_value,
                0 AS procedure_source_concept_id, -- Need concept id mapping
                NULL AS modifier_source_value
            FROM {INTRAOP_SCHEMA}.operation AS operation
            JOIN {OMOP_SCHEMA}.person AS person on person.person_source_value = operation.anon_case_no
            LEFT JOIN {OMOP_SCHEMA}.provider AS provider on
            (provider.provider_source_value = operation.anon_surgeon_name or provider.provider_source_value = operation.anon_plan_anaesthetist_1_name or provider.provider_source_value = operation.anon_plan_anaesthetist_2_name) 
        ) AS temp
    ),
    post_op__renal AS (
        SELECT ROW_NUMBER() OVER (ORDER BY temp.dialysis_starttime asc) AS procedure_occurence_id, 
            temp.person_id, 
            temp.procedure_concept_id, 
            temp.procedure_date,
            temp.procedure_datetime,
            temp.procedure_end_date,
            temp.procedure_end_datetime,
            temp.procedure_type_concept_id,
            temp.modifier_concept_id,
            temp.quantity,
            temp.provider_id,
            temp.visit_occurrence_id,
            temp.visit_detail_id,
            temp.procedure_source_value,
            temp.procedure_source_concept_id,
            temp.modifier_source_value
        FROM (
            SELECT  DISTINCT
                person.person_id,
                CASE 
                    WHEN crrt_type = 'CVVHDF - Continuous Veno-Venous Hemodiafiltration' THEN 4049846
                    WHEN crrt_type = 'CVVHD - Continuous Veno-Venous Hemodialysis' THEN 4051329 
                    ELSE 0 
                END AS procedure_concept_id,
                renal.crrt_authored_date AS procedure_date,
                renal.dialysis_starttime AS dialysis_starttime,
                CONCAT(renal.crrt_authored_date, ' ', renal.dialysis_starttime) ::timestamp AS procedure_datetime,
                CAST(NULL as date) AS procedure_end_date,
                CAST(NULL as timestamp) AS procedure_end_datetime,
                32879 AS procedure_type_concept_id,
                0 AS modifier_concept_id,
                0 AS quantity,
                CAST(NULL as INTEGER) AS provider_id,
                CAST(NULL as INTEGER) AS visit_occurrence_id,
                0 AS visit_detail_id,
                renal.crrt_type AS procedure_source_value,
                CASE 
                    WHEN crrt_type = 'CVVHDF - Continuous Veno-Venous Hemodiafiltration' THEN 4049846
                    WHEN crrt_type = 'CVVHD - Continuous Veno-Venous Hemodialysis' THEN 4051329 
                    ELSE 0 
                END AS procedure_source_concept_id,
                NULL AS modifier_source_value
            FROM {POSTOP_SCHEMA}.renal AS renal
            JOIN {OMOP_SCHEMA}.person AS person ON person.person_source_value = renal.anon_case_no
        ) AS temp
    ),
    pre_op__radiology AS (
        SELECT ROW_NUMBER() OVER (ORDER BY procedure_date asc) AS procedure_occurence_id, temp.*
            FROM(
                SELECT  DISTINCT
                        person.person_id,
                        0 AS procedure_concept_id, -- Need concept id mapping
                        operation_startdate AS procedure_date,
                        CONCAT(operation_startdate, ' ', operation_starttime) ::timestamp AS procedure_datetime,
                        operation_enddate AS procedure_end_date,
                        CONCAT(operation_enddate, ' ', operation_endtime) ::timestamp AS procedure_end_datetime,
                        32879 AS procedure_type_concept_id,
                        0 AS modifier_concept_id,
                        0 AS quantity,
                        CAST(NULL as INTEGER) AS provider_id,
                        radiology.session_id AS visit_occurrence_id,
                        0 AS visit_detail_id,
                        procedure_name AS procedure_source_value,
                        0 AS procedure_source_concept_id, -- Need concept id mapping
                        NULL AS modifier_source_value
                FROM {PREOP_SCHEMA}.radiology AS radiology
                JOIN {OMOP_SCHEMA}.person AS person ON person.person_source_value = radiology.anon_case_no
            ) AS temp
    ),
    final AS (
        SELECT * FROM intra_op__operation
        UNION ALL
        SELECT  post_op__renal.procedure_occurence_id
                + (SELECT COUNT(*) FROM intra_op__operation)
                AS SrNo,
                post_op__renal.person_id, 
                post_op__renal.procedure_concept_id, 
                post_op__renal.procedure_date,
                post_op__renal.procedure_datetime,
                post_op__renal.procedure_end_date,
                post_op__renal.procedure_end_datetime,
                post_op__renal.procedure_type_concept_id,
                post_op__renal.modifier_concept_id,
                post_op__renal.quantity,
                post_op__renal.provider_id,
                post_op__renal.visit_occurrence_id,
                post_op__renal.visit_detail_id,
                post_op__renal.procedure_source_value,
                post_op__renal.procedure_source_concept_id,
                post_op__renal.modifier_source_value
        FROM post_op__renal
        UNION ALL
        SELECT  pre_op__radiology.procedure_occurence_id
                + (SELECT max(post_op__renal.procedure_occurence_id) FROM post_op__renal)
                + (SELECT max(intra_op__operation.procedure_occurence_id) FROM intra_op__operation) 
                AS SrNo,
                pre_op__radiology.person_id, 
                pre_op__radiology.procedure_concept_id, 
                pre_op__radiology.procedure_date,
                pre_op__radiology.procedure_datetime,
                pre_op__radiology.procedure_end_date,
                pre_op__radiology.procedure_end_datetime,
                pre_op__radiology.procedure_type_concept_id,
                pre_op__radiology.modifier_concept_id,
                pre_op__radiology.quantity,
                pre_op__radiology.provider_id,
                pre_op__radiology.visit_occurrence_id,
                pre_op__radiology.visit_detail_id,
                pre_op__radiology.procedure_source_value,
                pre_op__radiology.procedure_source_concept_id,
                pre_op__radiology.modifier_source_value 
        FROM pre_op__radiology
    )
    SELECT * FROM final