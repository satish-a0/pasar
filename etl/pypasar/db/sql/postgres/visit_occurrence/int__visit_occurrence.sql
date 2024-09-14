-- *******************************************************************
-- NAME: int__visit_occurrence.sql
-- DESC: Create the intermediate view - visit_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-14  1.00           Initial create
-- *******************************************************************

-- Create the intermediate view for the visit_occurrence table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.int__visit_occurrence AS
    -- Combine staging data with 'Person' and 'Care_site' tables
    WITH final AS (
        SELECT 
            stg__vo.visit_occurrence_id AS visit_occurrence_id,
            p.person_id AS person_id,
            stg__vo.visit_start_date AS visit_start_date,
            stg__vo.visit_end_date AS visit_end_date,
            cs.care_site_id AS care_site_id,
            stg__vo.visit_source_value AS visit_source_value,
            stg__vo.discharged_to_source_value AS discharged_to_source_value
        FROM {OMOP_SCHEMA}.stg__visit_occurrence AS stg__vo
        LEFT JOIN {OMOP_SCHEMA}.person AS p
            ON stg__vo.anon_case_no = p.person_source_value
        LEFT JOIN {OMOP_SCHEMA}.care_site AS cs
            ON stg__vo.institution_code = cs.care_site_source_value
    )
    SELECT
        visit_occurrence_id,
        person_id,
        visit_start_date,
        visit_end_date,
        care_site_id,
        visit_source_value,
        discharged_to_source_value
    FROM final;