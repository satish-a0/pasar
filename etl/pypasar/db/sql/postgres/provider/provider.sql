-- *******************************************************************
-- NAME: provider.sql
-- DESC: Final table - provider
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-07  1.00           Initial create
-- 2024-09-07  2.00           Insert data into provider table from staging view
-- 2024-09-07 3.00            Updated the schema name
-- *******************************************************************


INSERT INTO {OMOP_SCHEMA}.provider
(
    provider_id,
    provider_name,
    npi,
    dea,
    specialty_concept_id,
    care_site_id,
    year_of_birth,
    gender_concept_id,
    provider_source_value,
    specialty_source_value,
    specialty_source_concept_id,
    gender_source_value,
    gender_source_concept_id
)
SELECT  provider_id,
        NULL,
        NULL,
        NULL,
        NULL,
        38004515,
        NULL,
        NULL,
        name,
        specialty,
        NULL,
        NULL,
        NULL
FROM {OMOP_SCHEMA}.stg__provider;

        