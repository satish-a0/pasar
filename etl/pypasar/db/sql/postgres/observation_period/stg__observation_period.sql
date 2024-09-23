-- Create the staging view for the observation_table, assigning a unique anon_case_no
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__observation_period AS
    
    
    WITH pre_op_char_Start_Date AS
        -- CTE for Pre_op_Char
        (
            SELECT 
                anon_case_no AS person_id,
                LEAST(
                    operation_startDate,
                    session_startdate
                ) AS start_date
                FROM {PREOP_SCHEMA}.char
        ),
        pre_op_char_End_Date AS
        (
            SELECT    
                anon_case_no AS person_id,
                GREATEST(
                    operation_enddate,
                    session_enddate
                ) AS end_date
            FROM {PREOP_SCHEMA}.char
        ),
        read_from_pre_op_char AS
        (
            SELECT
                char_start.person_id AS person_id,
                char_start.start_date AS observation_period_start_date,
                char_end.end_date AS observation_period_end_date,
                32879 AS period_type_concept_id
            FROM pre_op_char_Start_Date char_start
            INNER JOIN pre_op_char_End_Date char_end
            ON char_start.person_id = char_end.person_id
        ),


        -- CTE for Post_op_Discharge
        post_op_discharge_Start_Date AS
        (
            SELECT 
                anon_case_no AS person_id,
                LEAST(
                    operation_startDate,
                    diagnosis_date
                ) AS start_date
            FROM {POSTOP_SCHEMA}.discharge
        ),
        post_op_discharge_End_Date AS
        (
            SELECT    
                anon_case_no AS person_id,
                GREATEST(
                    operation_enddate,
                    diagnosis_date
                ) AS end_date
            FROM {POSTOP_SCHEMA}.discharge
        ),
        read_from_post_op_discharge AS
        (
            SELECT
                discharge_start.person_id AS person_id,
                discharge_start.start_date AS observation_period_start_date,
                discharge_end.end_date AS observation_period_end_date,
                32879 AS period_type_concept_id
            FROM post_op_discharge_Start_Date discharge_start
            INNER JOIN post_op_discharge_End_Date discharge_end
            ON discharge_start.person_id = discharge_end.person_id
        ),


        -- CTE for Post_op__Info
        post_op_info_Start_Date AS
        (
            SELECT 
                anon_case_no AS person_id,
                operation_startDate AS start_date
            FROM {POSTOP_SCHEMA}.info
        ),
        post_op_info_End_Date AS
        (
            SELECT    
                anon_case_no AS person_id,
                GREATEST(
                    operation_enddate,
                    death_date
                ) AS end_date
            FROM {POSTOP_SCHEMA}.info
        ),
        read_from_post_op_info AS
        (
            SELECT
                info_start.person_id AS person_id,
                info_start.start_date AS observation_period_start_date,
                info_end.end_date AS observation_period_end_date,
                32879 AS period_type_concept_id
            FROM post_op_info_Start_Date info_start
            INNER JOIN post_op_info_End_Date info_end
            ON info_start.person_id = info_end.person_id
        ),


        -- CTE for Post_op__lab_micro
        post_op_lab_micro_Start_Date AS
        (
            SELECT 
                anon_case_no AS person_id,
                LEAST(
                    labmicro.operation_startDate,
                    labmicro.reported_date
                ) AS start_date
            FROM {POSTOP_SCHEMA}.labmicro
        ),
        post_op_lab_micro_End_Date AS
        (
            SELECT    
                anon_case_no AS person_id,
                operation_enddate AS end_date
            FROM {POSTOP_SCHEMA}.info
        ),
        read_from_post_op_lab_micro AS
        (
            SELECT
                micro_start.person_id  AS person_id,
                micro_start.start_date AS observation_period_start_date,
                micro_end.end_date AS observation_period_end_date,
                32879 AS period_type_concept_id
            FROM post_op_lab_micro_Start_Date micro_start
            INNER JOIN post_op_lab_micro_End_Date micro_end
            ON micro_start.person_id = micro_end.person_id
        ),

        
        -- CTE for pre_op_lab
        preop_lab_Start_Date AS
        (
            SELECT 
                anon_case_no AS person_id,
                operation_startDate AS start_date
            FROM {PREOP_SCHEMA}.lab
        ),
        preop_lab_End_Date AS
        (
            SELECT    
                anon_case_no AS person_id,
                operation_enddate AS end_date
            FROM {PREOP_SCHEMA}.lab
        ), 
        read_from_pre_op_lab AS
        (
            SELECT
                lab_start.person_id AS person_id,
                lab_start.start_date AS observation_period_start_date,
                lab_end.end_date AS observation_period_end_date,
                32879 AS period_type_concept_id
            FROM preop_lab_Start_Date lab_start
            INNER JOIN preop_lab_End_Date lab_end
            ON lab_start.person_id = lab_end.person_id
        ),

        -- Union all table before assign observation_period_id at final CTE
        union_all_tables AS
        (
            SELECT * FROM read_from_pre_op_char
            UNION ALL
            SELECT * FROM read_from_post_op_discharge
            UNION ALL
            SELECT * FROM read_from_post_op_info
            UNION ALL
            SELECT * FROM read_from_post_op_lab_micro
            UNION ALL
            SELECT * FROM read_from_pre_op_lab
        ),
        final AS
        (
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY anon_case_no, id
                    ) AS observation_period_id,
                *
            FROM union_all_tables
        )
        SELECT
            observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            period_type_concept_id
        FROM final;