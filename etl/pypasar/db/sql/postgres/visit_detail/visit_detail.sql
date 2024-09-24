-- *******************************************************************
-- NAME: visit_detail.sql
-- DESC: Final table - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ------ ----  ----  -------  ----------------------------------------
-- 2024-09-24  1.00            Initial create
-- *******************************************************************

INSERT INTO {OMOP_SCHEMA}.visit_detail (
    visit_detail_id,
    person_id,
    visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,
    provider_id,
    care_site_id,
    visit_detail_source_value,
    visit_detail_source_concept_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharged_to_source_value,
    discharged_to_concept_id,
    preceding_visit_detail_id,
    parent_visit_detail_id,
    visit_occurrence_id
)
SELECT
    -- Assign a unique visit_detail_id to each combination of session_startdate and id
    ROW_NUMBER() OVER (ORDER by session_startdate, id) AS visit_detail_id,
    person_id,
    32037 AS visit_detail_concept_id, 
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    32879 ASvisit_detail_type_concept_id, 
    provider_id,
    care_site_id,
    'ICU'::text AS visit_detail_source_value,
    NULL AS visit_detail_source_concept_id,
    NULL AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    NULL AS discharged_to_source_value,
    NULL AS discharged_to_concept_id,
    NULL AS preceding_visit_detail_id,
    NULL AS parent_visit_detail_id,
    visit_occurrence_id
FROM {OMOP_SCHEMA}.int__visit_detail;