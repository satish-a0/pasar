-- *******************************************************************
-- NAME: stg__visit_detail.sql
-- DESC: Create the staging view - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-18  1.00           Initial create
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
        FROM postop.icu
        WHERE icu_admission_date IS NOT NULL 
            AND icu_discharge_date IS NOT NULL
    ),
    -- Ensure only distinct rows with corresponding id and session_startdate
    filtered_postop__icu AS (
        SELECT *
        FROM postop__icu
        WHERE row_num = 1
    ),
    -- Extract distinct relevant columns from the intra_op.operation table
    intraop__operation AS (
        SELECT DISTINCT 
            session_id, 
            anon_case_no, 
            anon_surgeon_name
        FROM intraop.operation
    ),
    -- Combine the filtered_postop__icu and intraop__operation
    final AS (
        SELECT 
            icu.id AS id,
            icu.anon_case_no AS anon_case_no,
            icu.session_id AS session_id,
            icu.session_startdate AS session_startdate,
            icu.icu_admission_date AS visit_detail_start_date,
            icu.icu_admission_time AS visit_detail_start_datetime,
            icu.icu_discharge_date AS visit_detail_end_date,
            icu.icu_discharge_time AS visit_detail_end_datetime,
            icu.icu_location AS icu_location,
            op.anon_surgeon_name AS anon_surgeon_name
        FROM filtered_postop__icu AS icu
        LEFT JOIN intraop__operation AS op
            ON icu.anon_case_no = op.anon_case_no
            AND icu.session_id = op.session_id
    )

    SELECT
        anon_case_no,                   -- For mapping with the person_id and visit_occurrence_id fields
        session_id,                     -- For generating IDs and mapping with the visit_occurrence_id field
        session_startdate,              -- For generating IDs
        visit_detail_start_date,
        visit_detail_start_datetime,
        visit_detail_end_date,
        visit_detail_end_datetime,
        icu_location,                   -- For mapping with the care_site_id field
        anon_surgeon_name               -- For mapping with the provider_id field
    FROM final