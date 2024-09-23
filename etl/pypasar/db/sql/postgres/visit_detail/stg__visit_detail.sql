-- Create or replace the staging view for visit_detail
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__visit_detail AS
    -- Extract relevant columns from the pre_op.char table
    -- Create or replace the staging view for visit_detail

    -- Extract relevant columns from the pre_op.char table
    WITH source AS (
        SELECT DISTINCT
            -- po.id AS po_id, 
            po.session_startdate AS po_session_startdate,
            po.anon_case_no,
            po.session_id,
            po.icu_admission_date,
            COALESCE((po.icu_admission_date + operation_starttime::interval), '2000-01-01') AS visit_detail_start_datetime,
            COALESCE((po.icu_discharge_date + operation_endtime::interval), '2000-01-01') AS visit_detail_end_datetime,
            po.icu_discharge_date,
            po.icu_location
        FROM postop.icu po
        WHERE icu_admission_date is not null 
    
    ),

   unique_operation AS (
   SELECT DISTINCT session_id, anon_case_no, anon_surgeon_name
        FROM intraop.operation
        WHERE anon_surgeon_name is not null
    ),
   
   mapped AS (
        SELECT opr.anon_surgeon_name,
               person.person_id,
               s.session_id,
               s.anon_case_no,
               s.icu_admission_date,
               s.visit_detail_start_datetime,
               s.icu_discharge_date,
               s.visit_detail_end_datetime,
               s.icu_location
        FROM source AS s
		LEFT JOIN unique_operation opr
           ON s.anon_case_no = opr.anon_case_no AND s.session_id = opr.session_id
        LEFT JOIN omop_sqldev_schema.person 
           ON person.person_source_value = s.anon_case_no
        WHERE icu_admission_date is not null and icu_discharge_date is not null
        ----------------------------mapped provider--------------------------------
        -- LEFT JOIN provider pd 
        --   ON s.anon_surgeon_name = pd.provider_source_value

        ---------------------------mapped care_site--------------------------------
        -- LEFT JOIN care_site cs 
        --   ON s.icu_location = cs.care_site__source_value 

        --------------------------mapped visit_occurrence--------------------------
        -- LEFT JOIN visit_occurrence vo 
        --   ON s.session_id = vo.session_id
        
    ),

    renamed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER by icu_admission_date , session_id) AS visit_detail_id,
        person_id,
        32037                                   AS visit_detail_concept_id,
        icu_admission_date                      AS visit_detail_start_date,
        visit_detail_start_datetime,
        icu_discharge_date                      AS visit_detail_end_date,
        visit_detail_end_datetime,
        32879                                   AS visit_detail_type_concept_id,
        NULL                                    AS provider_id,
        NULL                                    AS care_site_id,
        'ICU'                                   AS visit_detail_source_value,
        NULL                                    AS admitted_from_concept_id,
        NULL                                    AS admitted_from_source_value,
        NULL                                    AS discharged_to_source_value,
        NULL                                    AS preceding_visit_detail_id,
        NULL                                    AS parent_visit_detail_id,
        NULL                                    AS visit_occurrence_id
    FROM mapped
    )

    SELECT  * from source
    
    

