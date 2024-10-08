-- *******************************************************************
-- NAME: stg__visit_detail.sql
-- DESC: Create the staging view - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-18  1.00           Initial create
-- 2024-10-01  2.00           Ignore the intra_op.operation table
-- *******************************************************************

-- Create or replace the staging view for the visit_detail table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__visit_detail AS
    -- Extract relevant columns from the post_op.icu table
    WITH postop__icu AS (
        SELECT
            id,
            anon_case_no,
            session_id,
            session_startdate,
            icu_admission_date,
            icu_admission_time,
            icu_discharge_date,
            icu_discharge_time,
            icu_location,
            ROW_NUMBER() OVER (
                PARTITION BY anon_case_no, session_id, session_startdate, icu_admission_date, icu_admission_time, icu_discharge_date, icu_discharge_time, icu_location
                ORDER BY id
            ) AS row_num
        FROM {POSTOP_SCHEMA}.icu
        WHERE icu_admission_date IS NOT NULL 
            AND icu_discharge_date IS NOT NULL
    ),
    -- Ensure only distinct rows with corresponding id and session_startdate
    filtered_postop__icu AS (
        SELECT *
        FROM postop__icu
        WHERE row_num = 1
    ),
    -- Retrieve ICU visit details; combine date and time
    final AS (
        SELECT 
            icu.id AS id,
            icu.anon_case_no AS anon_case_no,
            icu.session_id AS session_id,
            icu.session_startdate AS session_startdate,
            icu.icu_admission_date AS visit_detail_start_date,
            (CONCAT(icu.icu_admission_date, ' ', COALESCE(icu.icu_admission_time::text, '00:00:00')))::timestamp AS visit_detail_start_datetime,
            icu.icu_discharge_date AS visit_detail_end_date,
            (CONCAT(icu.icu_discharge_date, ' ', COALESCE(icu.icu_discharge_time::text, '00:00:00')))::timestamp AS visit_detail_end_datetime,
            icu.icu_location AS icu_location
        FROM filtered_postop__icu AS icu
    )

    SELECT
        id,                             -- For generating IDs
        anon_case_no,                   -- For mapping with the person_id field
        session_startdate,              -- For generating IDs
        visit_detail_start_date,
        visit_detail_start_datetime,
        visit_detail_end_date,
        visit_detail_end_datetime,
        icu_location,                   -- For mapping with the care_site_id field
        session_id                      -- For mapping with the visit_occurrence_id field
    FROM final;