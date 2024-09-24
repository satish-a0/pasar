-- *******************************************************************
-- NAME: int__visit_detail.sql
-- DESC: Create the intermediate view - visit_detail
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-18  1.00           Initial create
-- *******************************************************************

-- Create the intermediate view for the visit_detail table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.int__visit_detail AS
    -- Convert visit_occurrence_id back to session_id
    WITH sessionIDs AS (
        SELECT CAST(LEFT(CAST(visit_occurrence_id AS TEXT), LENGTH(CAST(visit_occurrence_id AS TEXT)) - 2) AS INTEGER) AS session_id, *
        FROM {OMOP_SCHEMA}.visit_occurrence
    ),
    -- Combine staging data with other tables
    final AS (
        SELECT
            p.person_id AS person_id,
            stg__vd.visit_detail_start_date AS visit_detail_start_date,
            stg__vd.visit_detail_start_datetime AS visit_detail_start_datetime,
            stg__vd.visit_detail_end_date AS visit_detail_end_date,
            stg__vd.visit_detail_end_datetime AS visit_detail_end_datetime,
            pr.provider_id AS provider_id,
            cs.care_site_id AS care_site_id
        FROM {OMOP_SCHEMA}.stg__visit_detail AS stg__vd
        -- Join with the Person table
        LEFT JOIN {OMOP_SCHEMA}.person AS p
            ON stg__vd.anon_case_no = p.person_source_value
        -- Join with the Provider table
        LEFT JOIN {OMOP_SCHEMA}.provider AS pr
            ON stg__vd.anon_surgeon_name = pr.provider_source_value
        -- Join with the Care_site table
        LEFT JOIN {OMOP_SCHEMA}.care_site AS cs
            ON stg__vd.icu_location = cs.care_site_source_value
        -- Join with the Visit_occurrence table
        LEFT JOIN sessionIDs AS vo
            ON stg__vd.session_id = vo.session_id
    )

    SELECT
        person_id,
        visit_detail_start_date,
        visit_detail_start_datetime,
        visit_detail_end_date,
        visit_detail_end_datetime,
        provider_id,
        care_site_id
    FROM final,