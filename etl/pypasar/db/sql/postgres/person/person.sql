-- *******************************************************************
-- NAME: person.sql
-- DESC: Final table - person
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-08-27  1.00           Initial create
-- 2024-09-01  2.00           Insert data into person table from staging view
-- 2024-09-04  3.00           Updated the schema name
-- *******************************************************************

INSERT INTO {OMOP_SCHEMA}.person
(
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
)
SELECT
    person_id,
    gender_concept_id,
    year_of_birth,
    NULL AS month_of_birth,
    NULL AS day_of_birth,
    NULL AS birth_datetime,
    race_concept_id,
    0 AS ethnicity_concept_id,
    NULL AS location_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    person_source_value,
    gender_source_value,
    0 AS gender_source_concept_id,
    race_source_value,
    0 AS race_source_concept_id,
    NULL AS ethnicity_source_value,
    NULL AS ethnicity_source_concept_id
FROM {OMOP_SCHEMA}.stg__person;