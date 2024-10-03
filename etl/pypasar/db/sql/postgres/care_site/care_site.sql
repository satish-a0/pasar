-- *******************************************************************
-- NAME: care_site.sql
-- DESC: Final table - care_site
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-30  1.00           Initial create
-- *******************************************************************

INSERT INTO {OMOP_SCHEMA}.care_site
(
    care_site_id,
    care_site_name,
    place_of_service_concept_id,
    location_id,
    care_site_source_value,
    place_of_service_source_value
)    
SELECT
    care_site_id,
    care_site_name,
    place_of_service_concept_id,
    NULL AS location_id,
    care_site_source_value,
    place_of_service_source_value
FROM {OMOP_SCHEMA}.stg__care_site;