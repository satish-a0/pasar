-- *******************************************************************
-- NAME: stg__care_site.sql
-- DESC: Create the staging view - care_site
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-30  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the person table, assigning a unique person_id
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__care_site AS 
    -- Extract relevant columns from the intra_op.operation table
    -- This CTE retrieves distinct values for care site and place of service information
    WITH intraop__operation AS (
        SELECT DISTINCT
            ot_description AS care_site_name, 
            ot_location_code AS place_of_service_source_value, 
            ot_code AS care_site_source_value
        FROM {INTRAOP_SCHEMA}.operation
        UNION
        SELECT DISTINCT
            institution_code AS care_site_name, 
            institution_code AS place_of_service_source_value, 
            institution_code AS care_site_source_value
        FROM {INTRAOP_SCHEMA}.operation
    ),
    -- Extract distinct relevant columns from the post_op.discharge table
    postop__discharge AS (
        SELECT DISTINCT
            external_hospital_name AS care_site_name,
            external_hospital_code AS place_of_service_source_value,
            external_hospital_code AS care_site_source_value
        FROM {POSTOP_SCHEMA}.discharge
    ),
    -- Extract distinct relevant columns from the post_op.icu table
    postop__icu AS (
        SELECT DISTINCT
            icu_location AS care_site_name,
            icu_location AS place_of_service_source_value,
            icu_location AS care_site_source_value
        FROM {POSTOP_SCHEMA}.icu
        WHERE icu_location IS NOT NULL
    ),
    -- Combine data for all sources
    sources AS (
        SELECT * FROM intraop__operation
        UNION
        SELECT * FROM postop__discharge
        UNION
        SELECT * FROM postop__icu
    ),
    -- Map place of service source values to corresponding concept IDs
    mapping AS (
        SELECT
            *,
            CASE place_of_service_source_value
                WHEN 'ASC' THEN 8883
                WHEN 'AEC' THEN 38004220
                WHEN 'MOT' THEN 8718
                WHEN 'NHC' THEN 581383
                WHEN 'CICU' THEN 581383
                WHEN 'SGH' THEN 38004515
                ELSE 8717   -- For unmapped operating theaters
            END AS place_of_service_concept_id
        FROM sources
    )
    SELECT
        ROW_NUMBER() OVER(ORDER BY care_site_source_value) AS care_site_id,
        care_site_name,
        place_of_service_concept_id,
        care_site_source_value,
        place_of_service_source_value	
    FROM mapping;