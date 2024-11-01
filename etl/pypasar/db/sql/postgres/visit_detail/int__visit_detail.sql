-- *******************************************************************
-- NAME: int__visit_detail.sql
-- DESC: Create the intermediate view - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-18  1.00           Initial create
-- 2024-10-01  2.00           Remove the mapping with the provider table
-- *******************************************************************
-- Create the intermediate view for the visit_detail table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.int__visit_detail AS
    -- Convert visit_occurrence_id back to session_id
WITH sessionIDs AS (
    SELECT
        CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER) AS session_id,
        visit_occurrence_id,
        visit_start_date,
        visit_end_date
    FROM {OMOP_SCHEMA}.visit_occurrence
),
final AS (
    SELECT
        p.person_id AS person_id,
        -- Check if visit_detail_start_date on visit_occurrence use visit_start_date
        CASE
            WHEN stg__vd.visit_detail_start_date >= vo.visit_start_date
                 AND stg__vd.visit_detail_start_date <= vo.visit_end_date
            THEN stg__vd.visit_detail_start_date
            ELSE vo.visit_start_date
        END AS visit_detail_start_date,
        CASE
            WHEN stg__vd.visit_detail_start_datetime >= vo.visit_start_date
                 AND stg__vd.visit_detail_start_datetime <= vo.visit_end_date
            THEN stg__vd.visit_detail_start_datetime
            ELSE vo.visit_start_date
        END AS visit_detail_start_datetime,
        -- Check if visit_detail_end_date on visit_occurrence use visit_end_date
        CASE
            WHEN stg__vd.visit_detail_end_date >= vo.visit_start_date
                 AND stg__vd.visit_detail_end_date <= vo.visit_end_date
            THEN stg__vd.visit_detail_end_date
            ELSE vo.visit_end_date
        END AS visit_detail_end_date,
        CASE
            WHEN stg__vd.visit_detail_end_datetime >= vo.visit_start_date
                 AND stg__vd.visit_detail_end_datetime <= vo.visit_end_date
            THEN stg__vd.visit_detail_end_datetime
            ELSE vo.visit_end_date
        END AS visit_detail_end_datetime,
        cs.care_site_id AS care_site_id,
        vo.visit_occurrence_id AS visit_occurrence_id,
        stg__vd.id AS id,
        stg__vd.session_startdate AS session_startdate
    FROM {OMOP_SCHEMA}.stg__visit_detail AS stg__vd
    LEFT JOIN {OMOP_SCHEMA}.person AS p
        ON stg__vd.anon_case_no = p.person_source_value
    LEFT JOIN {OMOP_SCHEMA}.care_site AS cs
        ON stg__vd.icu_location = cs.care_site_source_value
    LEFT JOIN sessionIDs AS vo
        ON stg__vd.session_id = vo.session_id
)
SELECT
    person_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    care_site_id,
    visit_occurrence_id,
    id,
    session_startdate
FROM final;