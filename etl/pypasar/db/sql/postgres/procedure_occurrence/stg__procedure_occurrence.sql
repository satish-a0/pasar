-- *******************************************************************
-- NAME: stg__procedure_occurrence.sql
-- DESC: Create the staging view - procedure_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-08-27  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the procedure_occurrence table, assigning a unique procedure_occurrence_id
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__procedure_occurrence AS
    with intra_op__operation as (
        select 
            ROW_NUMBER() OVER (ORDER BY operation_startdate, id asc) AS procedure_occurence_id,
            person.person_id,
            '' as procedure_concept_id, -- Need concept id mapping
            operation.operation_startdate as procedure_date,
            CONCAT(operation.operation_startdate, ' ', operation.operation_starttime) ::timestamp as procedure_datetime,
            operation.operation_enddate as procedure_end_date,
            CONCAT(operation.operation_enddate, ' ', operation.operation_endtime) ::timestamp as procedure_end_datetime,
            32879 as procedure_type_concept_id,
            NULL as modifier_concept_id,
            NULL as quantity,
            provider.provider_id as provider_id, -- clarify
            operation.session_id as visit_occurrence_id, -- visit_occurrence_id (session_id)
            NULL as visit_detail_id,
            operation.procedure_code as procedure_source_value,
            0 as procedure_source_concept_id, -- Need concept id mapping
            NULL as modifier_source_value
        from intraop.operation as operation
        join omop_sqldev_schema.person as person on person.person_source_value = operation.anon_case_no
        left join omop_sqldev_schema.provider as provider on (provider.provider_source_value = operation.anon_surgeon_name or provider.provider_source_value = operation.anon_plan_anaesthetist_1_name or provider.provider_source_value = operation.anon_plan_anaesthetist_2_name)
    ),
    with post_op__renal as (
        select 
            ROW_NUMBER() OVER (ORDER BY id, renal.dialysis_starttime asc) AS procedure_occurence_id,
            person.person_id,
            renal.crrt_type as procedure_concept_id,
            renal.crrt_authored_date as procedure_date,
            CONCAT(renal.crrt_authored_date, ' ', renal.dialysis_starttime) ::timestamp as procedure_datetime,
            NULL as procedure_end_date,
            NULL as procedure_end_datetime,
            32879 as procedure_type_concept_id,
            NULL as modifier_concept_id,
            NULL as quantity,
            NULL as provider_id,
            NULL as visit_occurrence_id,
            NULL as visit_detail_id,
            renal.crrt_type as procedure_source_value,
            0 as procedure_source_concept_id,
            NULL as modifier_source_value
        from postop.renal as renal
        join omop_sqldev_schema.person as person on person.person_source_value = renal.anon_case_no

    )