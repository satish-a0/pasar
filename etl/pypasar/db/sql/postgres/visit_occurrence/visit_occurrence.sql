-- *******************************************************************
-- NAME: visit_occurrence.sql
-- DESC: Final table - visit_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-14  1.00           Initial create
-- 2024-09-21  2.00           Set the default visit_concept_id to 0 — need to map to the standard concept IDs
-- 2024-09-22  3.00           Set the value of care_site_id if it is not specified in the care_site table
-- *******************************************************************

INSERT INTO {OMOP_SCHEMA}.visit_occurrence
(
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_occurrence_id
)
SELECT
    visit_occurrence_id,
    person_id,
    0 AS visit_concept_id,
    visit_start_date,
    NULL AS visit_start_datetime,
    visit_end_date,
    NULL AS visit_end_datetime,
    32879 AS visit_type_concept_id,
    NULL AS provider_id,
    COALESCE(38004515, care_site_id) AS care_site_id,
    visit_source_value,
    NULL AS visit_source_concept_id,
    NULL AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    NULL AS discharged_to_concept_id,
    discharged_to_source_value,
    NULL AS preceding_visit_occurrence_id
FROM {OMOP_SCHEMA}.int__visit_occurrence