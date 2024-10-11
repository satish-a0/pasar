-- ***********************************************************************
-- NAME: temp_drug_drug_view.sql
-- Create temp view that joins intraop.drugdrug with source_to_concept_map
-- ***********************************************************************


create or replace view {OMOP_SCHEMA}.{DRUGDRUG_STCM_VIEW} as
with 
dd as (
	select 
		id,
		anon_case_no_clindoc,
		anon_case_no,
		session_id,
		operation_id,
		coalesce(infusion_startdatetime, operation_startdate)::timestamp as drug_exposure_start_date,
		coalesce(
			infusion_startdatetime,
			(operation_startdate::text || ' ' || operation_starttime)::timestamp
		) as drug_exposure_start_datetime,
		operation_enddate as drug_exposure_end_date,
		(operation_enddate::text || ' ' || operation_endtime)::timestamp as drug_exposure_end_datetime,
		volume as quantity,
		concentration, -- not used in stcm
		drug_name,
		upper(drug_name) as "standardized_drug_name",
		'intraop' as source_schema,
		'drugdrug' as source_table
	from intraop.drugdrug -- 2469 records
	where drug_name is not null -- 125 records
    ), -- 2344 records

stcm as (
	select 
		row_number() over (partition by source_code order by target_concept_id) as "source_code_rn",
		* 
	from omop_sqldev_schema.source_to_concept_map stcm
	where source_vocabulary_id = 'SG_PASAR_INTRAOP_DRUG_DRUG' 
    ), -- 258 records
    
dd_stcm as (
    select
    stcm.source_code,
    stcm.target_concept_id,
    dd.*
    from dd
    left join stcm  
    on stcm.source_code = dd.standardized_drug_name
    and stcm.source_code_rn = 1 -- remove duplicates
    ) -- 2344 records

select 
    id,
    anon_case_no_clindoc,
    anon_case_no,
    session_id,
    operation_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    quantity,
    drug_name as drug_source_value,
    target_concept_id as drug_concept_id,
    source_schema,
    source_table
from dd_stcm -- Total: 2344 records