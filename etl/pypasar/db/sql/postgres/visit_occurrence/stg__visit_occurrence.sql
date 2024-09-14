-- *******************************************************************
-- NAME: stg__visit_occurrence.sql
-- DESC: Create the staging view - visit_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-14  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the visit_occurrence table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__visit_occurrence AS
    -- Extract relevant columns from the pre_op.char table
    WITH preop__char AS (
        SELECT
            session_id AS visit_occurrence_id,
            anon_case_no,
            admission_type AS visit_source_value,
            session_startdate AS visit_start_date,
            session_enddate AS visit_end_date,
            institution_code
        FROM {PREOP_SCHEMA}.char
        -- To ensure unique rows
        GROUP BY visit_occurrence_id, anon_case_no, visit_source_value, visit_start_date, visit_end_date, institution_code
    ),
    -- Extract distinct relevant columns from the post_op.discharge table
    postop__discharge AS (
        SELECT DISTINCT
            anon_case_no,
            external_hospital_code AS discharged_to_source_value
        FROM {POSTOP_SCHEMA}.discharge
    ),
    -- Combine the preop__char and postop__discharge
    final AS (
        SELECT
            c.visit_occurrence_id AS visit_occurrence_id,
            c.anon_case_no AS anon_case_no, 
            c.visit_source_value AS visit_source_value, 
            c.visit_start_date AS visit_start_date, 
            c.visit_end_date AS visit_end_date, 
            c.institution_code AS institution_code,
            d.discharged_to_source_value AS discharged_to_source_value
        FROM preop__char as c
        -- Ensure all records from preop__char are included, which will make up the person table
        LEFT JOIN postop__discharge AS d
            ON c.anon_case_no = d.anon_case_no
        ORDER BY c.visit_occurrence_id
    )
    SELECT
        visit_occurrence_id,
        anon_case_no,               -- For mapping with the person_id field
        visit_source_value,         -- For mapping with the visit_concept_id field
        visit_start_date,
        visit_end_date,
        institution_code,           -- For mapping with the care_site_id field
        discharged_to_source_value
    FROM final;