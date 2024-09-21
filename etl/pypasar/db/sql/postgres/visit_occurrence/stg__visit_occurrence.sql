-- *******************************************************************
-- NAME: stg__visit_occurrence.sql
-- DESC: Create the staging view - visit_occurrence
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-14  1.00           Initial create
-- 2024-09-21  2.00           Add a number suffix to ensure visit_occurrence_id is unique
-- *******************************************************************

-- Create the staging view for the visit_occurrence table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__visit_occurrence AS
    -- Extract relevant columns from the pre_op.char table
    WITH preop__char AS (
        SELECT
            anon_case_no,
            session_id,
            admission_type AS visit_source_value,
            session_startdate AS visit_start_date,
            session_enddate AS visit_end_date,
            institution_code
        FROM {PREOP_SCHEMA}.char
        -- To ensure unique rows
        GROUP BY anon_case_no, session_id, visit_source_value, visit_start_date, visit_end_date, institution_code
    ),
    -- Extract distinct relevant columns from the post_op.discharge table
    postop__discharge AS (
        SELECT DISTINCT
            anon_case_no,
            session_id,
            external_hospital_code AS discharged_to_source_value
        FROM {POSTOP_SCHEMA}.discharge
    ),
    -- Combine the preop__char and postop__discharge
    final AS (
        SELECT
            c.session_id AS session_id,
            c.anon_case_no AS anon_case_no, 
            c.visit_source_value AS visit_source_value, 
            c.visit_start_date AS visit_start_date, 
            c.visit_end_date AS visit_end_date, 
            c.institution_code AS institution_code,
            d.discharged_to_source_value AS discharged_to_source_value
        FROM preop__char as c
        -- Ensure all records from preop__char are included, which will make up the person table
        LEFT JOIN postop__discharge AS d
            -- Ensure only matching anon_case_no and session_id are included
            ON c.anon_case_no = d.anon_case_no
            AND c.session_id = d.session_id
        ORDER BY c.session_id
    )
    SELECT
        CASE
            WHEN COUNT(*) OVER (PARTITION BY session_id) = 1 THEN
                -- If the session_id is unique, concatenate it with '00'
                CAST(session_id || '00' AS INTEGER)
            ELSE
                -- If not unique, concatenate with a padded row number
                CAST(
                    session_id || LPAD(CAST(ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY visit_start_date) AS TEXT), 2, '0')
                    AS INTEGER
                )
        END AS visit_occurrence_id,
        anon_case_no,               -- For mapping with the person_id field
        visit_source_value,         -- For mapping with the visit_concept_id field
        visit_start_date,
        visit_end_date,
        institution_code,           -- For mapping with the care_site_id field
        discharged_to_source_value
    FROM final;