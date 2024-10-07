-- *******************************************************************
-- NAME: specimen.sql
-- DESC: Final table - specimen
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-30  1.00           Initial create
-- *******************************************************************

INSERT INTO {OMOP_SCHEMA}.specimen
(
    specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
)
SELECT
    specimen_id,
    person_id,
    0 AS specimen_concept_id,    -- TODO: Mapping for the specimen_concept_id field
    32879 AS specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    NULL AS quantity,
    NULL AS unit_concept_id,
    NULL AS anatomic_site_concept_id,
    NULL AS disease_status_concept_id,
    NULL AS specimen_source_id,
    specimen_source_value,
    NULL AS unit_source_value,
    NULL AS anatomic_site_source_value,
    NULL AS disease_status_source_value
FROM {OMOP_SCHEMA}.stg__specimen;