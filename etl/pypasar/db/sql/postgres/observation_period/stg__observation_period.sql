-- Create the staging view for the observation_table, assigning a unique anon_case_no
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__observation_period AS
-- Create the staging view for the observation_table, assigning a unique anon_case_no
    WITH pre_op_char_source AS
        (
            SELECT
                anon_case_no AS person_id,
                MIN(LEAST(session_startdate, visit_date)) AS start_date,
                MAX(GREATEST(session_enddate, operation_enddate)) AS end_date,
                32879 AS period_type_concept_id
            FROM {PREOP_SCHEMA}.char
            GROUP BY anon_case_no
        ),

        -- CTE for Post_op_Discharge
        post_op_discharge_source AS
        (
            SELECT 
                anon_case_no AS person_id,
                MIN(LEAST(operation_startDate, diagnosis_date)) AS start_date,
                MAX(operation_enddate) AS end_date,
                32879 AS period_type_concept_id
            FROM {POSTOP_SCHEMA}.discharge
            GROUP BY anon_case_no
        ),

        -- CTE for Post_op__Info
        post_op_info_source AS
        (
            SELECT 
                anon_case_no AS person_id,
                MIN(operation_startDate) AS start_date,
                MAX(GREATEST(operation_enddate, death_date)) AS end_date,
                32879 AS period_type_concept_id
            FROM {POSTOP_SCHEMA}.info
            GROUP BY anon_case_no
        ),


        -- CTE for Post_op__lab_micro
        post_op_lab_micro_source AS
        (
            SELECT 
                anon_case_no AS person_id,
                MIN(LEAST(operation_startDate, reported_date))AS start_date,
                MAX(operation_enddate) AS end_date,
                32879 AS period_type_concept_id
            FROM {POSTOP_SCHEMA}.labmicro
            GROUP BY anon_case_no        
        ),


        -- CTE for pre_op_lab
        preop_lab_source AS
        (
            SELECT 
                anon_case_no AS person_id,
                MIN(LEAST(operation_startDate, preop_lab_collection_datetime))AS start_date,
                MAX(operation_enddate) AS end_date,
                32879 AS period_type_concept_id
            FROM {PREOP_SCHEMA}.lab
            GROUP BY anon_case_no
        ),

        -- Union all table before assign observation_period_id at final CTE
        union_all_tables AS
        (
            SELECT * FROM pre_op_char_source
            UNION ALL
            SELECT * FROM post_op_discharge_source
            UNION ALL
            SELECT * FROM post_op_info_source
            UNION ALL
            SELECT * FROM post_op_lab_micro_source
            UNION ALL
            SELECT * FROM preop_lab_source
        ),

        no_observation_period_id AS
        (
            SELECT
                person_id,
                MIN(start_date::date) AS start_date,
                MAX(end_date::date) AS end_date,
                32879 AS period_type_concept_id
            FROM union_all_tables
            GROUP BY person_id
        ),

        observation_period_before_join AS(
            SELECT
                ROW_NUMBER() OVER (ORDER BY start_date, person_id) AS observation_period_id,
                *
            FROM no_observation_period_id
            ORDER BY start_date
        ),
        
        final AS (
            SELECT
                op.observation_period_id,
                p.person_id,
                op.start_date,
                op.end_date,
                op.period_type_concept_id
            FROM observation_period_before_join AS op
            JOIN omop_sqldev_schema.person AS p
            ON op.person_id = p.person_source_value
            ORDER BY op.observation_period_id, p.person_id
        )


SELECT * FROM final;