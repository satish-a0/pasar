-- *******************************************************************
-- NAME: visit_detail.sql
-- DESC: Final table - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ------ ----  ----  -------  ----------------------------------------
-- 2024-09-17  1.00           Initial create and data insertion
-- *******************************************************************

-- Insert data into visit_detail from post_op__icu
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
    visit_detail_id,
    person_id,
    visit_detail_concept_id,  -- Intensive Care Concept ID
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,  -- Registry Concept ID
    NULL AS provider_id,
    NULL AS care_site_id,
    visit_detail_source_value,
    NULL AS visit_detail_source_concept_id,
    NULL AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    NULL AS discharged_to_source_value,
    NULL AS discharged_to_concept_id,
    NULL AS preceding_visit_detail_id,
    NULL AS parent_visit_detail_id,
    NULL AS visit_occurrence_id
FROM {OMOP_SCHEMA}.stg__visit_detail