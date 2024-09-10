-- *******************************************************************
-- NAME: stg__person.sql
-- DESC: Create the staging view - person
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-08-27  1.00           Initial create
-- 2024-09-01  2.00           Updated to use DENSE_RANK() for person_id
-- 2024-09-02  3.00           Updated the schema name
-- 2024-09-04  4.00           Updated CTEs to use filteredSource and prioritized non-null data selection
-- *******************************************************************

-- Create the staging view for the person table, assigning a unique person_id
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__person AS
    -- Extract relevant columns from the pre_op.char table
    WITH source AS (
        SELECT 
            anon_case_no AS person_source_value,
            session_startdate,
            operation_startdate,
            age_time_of_surgery,
            gender AS gender_source_value,
            race AS race_source_value,
            ROW_NUMBER() OVER (
                PARTITION BY anon_case_no 
                ORDER BY 
                    session_startdate, -- Primary sort on session_startdate
                    CASE WHEN gender IS NOT NULL THEN 0 ELSE 1 END, -- Prioritize non-null gender
                    CASE WHEN race IS NOT NULL THEN 0 ELSE 1 END -- Prioritize non-null race
            ) AS row_num
        FROM {PREOP_SCHEMA}.char
    ),
    -- Assign a unique person_id to each distinct person_source_value
    filteredSource AS (
        -- Assign person_id by selecting from the earliest for each person_source_value
        SELECT *, ROW_NUMBER() OVER (ORDER BY session_startdate, person_source_value) AS person_id
        FROM source s
        WHERE s.row_num = 1
    ), 
    -- Map gender and race values to their respective concept IDs
    mapping AS (
        SELECT
            person_source_value,
            CASE gender_source_value
                WHEN 'MALE' THEN 8507
                WHEN 'FEMALE' THEN 8532
                ELSE 0
            END AS gender_concept_id,
            CASE race_source_value
                WHEN 'Chinese' THEN 38003579
                WHEN 'Indian' THEN 38003574
                WHEN 'Malay' THEN 38003587
                WHEN 'Singaporean' THEN 38003596
                ELSE 0
            END AS race_concept_id
        FROM filteredSource
    ), 
    -- Calculate the year of birth
    computing AS (
        SELECT
            person_source_value,
            EXTRACT(YEAR FROM operation_startdate)::int - age_time_of_surgery::int AS year_of_birth
        FROM filteredSource
    ), 
    -- Combine the mapped gender and race with the calculated year of birth
    final AS (
        SELECT 
            fs.person_source_value AS person_source_value,
            fs.gender_source_value AS gender_source_value,
            fs.race_source_value AS race_source_value,
            fs.person_id AS person_id,
            m.gender_concept_id AS gender_concept_id,
            m.race_concept_id AS race_concept_id,
            c.year_of_birth AS year_of_birth
        FROM filteredSource AS fs
        JOIN mapping AS m
            ON fs.person_source_value = m.person_source_value
        JOIN computing AS c
            ON fs.person_source_value = c.person_source_value
        ORDER BY fs.person_id
    )
    SELECT
        person_id,
        gender_concept_id,
        year_of_birth,
        race_concept_id,
        person_source_value,
        gender_source_value,
        race_source_value
    FROM final;