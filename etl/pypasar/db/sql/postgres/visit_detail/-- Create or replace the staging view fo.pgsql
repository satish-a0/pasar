-- Create or replace the staging view for visit_detail

    -- Extract relevant columns from the pre_op.char table
    WITH source AS (
        SELECT 
            po.id AS po_id, 
            po.session_startdate AS po_session_startdate,
            po.anon_case_no,
            po.session_id,
            --omp.person_source_value,
            --omp.person_id,
            --vio.visit_source_value,
            -- opr.session_id AS opr_session_id,
            -- opr.anon_surgeon_name AS surgeon_name,
            -- opr.anon_plan_anaesthetist_1_name AS anaesthetist_1_name,
            -- opr.anon_plan_anaesthetist_2_name AS anaesthetist_2_name,
            po.icu_admission_date,
            po.icu_admission_time,
            po.operation_starttime,
            po.operation_endtime,
            po.icu_discharge_date,
            po.icu_discharge_time,
            po.icu_location
        FROM postop.icu po
        WHERE icu_admission_date is not null
        --LEFT JOIN omop_sqldev_schema.person omp 
            --ON omp.person_source_value = po.anon_case_no
        --LEFT JOIN omop_sqldev_schema.visit_occurrence vio 
            --ON po.anon_case_no = vio.visit_source_value 
		-- LEFT JOIN intraop.operation opr
        --     ON po.anon_case_no = opr.anon_case_no 
       
        
    ),

    mapping AS (
        SELECT 
            po_id, 
            -- po_session_startdate,
            -- anon_case_no,
            -- session_id,
            -- po.icu_admission_date,
            -- po.icu_admission_time,
            -- po.operation_starttime,
            -- po.operation_endtime,
            -- po.icu_discharge_date,
            -- po.icu_discharge_time,
            -- icu_location,
            -- CASE 
            --     WHEN Admission_Type = 'Inpatient' THEN 9201 
            --     WHEN Admission_Type = 'Day Surgery (DS)' THEN 9202 
            --     WHEN Admission_Type = 'Same Day Admission (SDA)' THEN 9203
            --     ELSE NULL
            -- END AS visit_detail_concept_id,
            32037 AS visit_detail_concept_id,
            -- if icu_admission_date,icu_discharge_date is null values is 2000-01-01
            COALESCE(icu_admission_date, '2000-01-01') AS check_icu_admission_date,
            COALESCE((icu_admission_date + operation_starttime::interval), '2000-01-01') AS concat_start_datetime,
            COALESCE(icu_discharge_date, '2000-01-01') AS check_icu_discharge_date,
            COALESCE((icu_discharge_date + operation_endtime::interval), '2000-01-01') AS concat_end_datetime,
            32879 AS visit_detail_type_concept_id,
            'ICU' AS visit_detail_source_value
            
        FROM source
    ),

  
    final AS (
        SELECT 
            s.po_id, 
            s.po_session_startdate,
            s.anon_case_no,
            s.session_id,
            s.icu_location,
            --fs.unique_visit_detail_id AS visit_detail_id,
            --m.person_id AS person_id,
            m.visit_detail_concept_id,
            m.visit_detail_type_concept_id,
            m.check_icu_admission_date AS visit_detail_start_date,
            m.concat_start_datetime AS visit_detail_start_datetime,
            m.check_icu_discharge_date AS visit_detail_end_date,
            m.concat_end_datetime AS visit_detail_end_datetime,
            m.visit_detail_source_value
            --fs.unique_provider_id AS provider_id,
            --fs.unique_visit_occurrence_id AS visit_occurrence_id
        FROM  source s
        LEFT JOIN mapping m
        ON s.po_id = m.po_id

        
    ), stg__visit_detail AS (

    SELECT 
            po_id, 
            po_session_startdate,
            anon_case_no,
            session_id,
            icu_location,
            visit_detail_concept_id,
            visit_detail_type_concept_id,
            visit_detail_start_date,
            visit_detail_start_datetime,
            visit_detail_end_date,
            visit_detail_end_datetime,
            visit_detail_source_value
    FROM   final
    ), 
   
   ----int__visit_detail---
   distinct_opr AS (

   SELECT DISTINCT session_id, anon_case_no, anon_surgeon_name
        FROM intraop.operation
    )  ,
   

   
   mapped__operation AS (
        SELECT stg__vd.* 
            --stg__vd.person_source_value,
            --stg__vd.person_id
            --vio.visit_source_value,
            --vio.visit_occurrence_id
        FROM stg__visit_detail AS stg__vd
        --LEFT JOIN omop_sqldev_schema.person omp 
        --   ON omp.person_source_value = stg__vd.anon_case_no
        -- LEFT JOIN omop_sqldev_schema.visit_occurrence vio 
        --     ON omp.person_id = vio.person_id 
		LEFT JOIN distinct_opr opr
           ON stg__vd.anon_case_no = opr.anon_case_no
        
    )

    SELECT
        *

    FROM  mapped__operation
    LIMIT 1000