-- ***********************************************************************
-- NAME: temp_drug_med_view.sql
-- Create temp view that joins intraop.drugmed with source_to_concept_map
-- ***********************************************************************


create or replace view {OMOP_SCHEMA}.{DRUGMED_STCM_VIEW} as

-- only keep 1 record of source_code = CEFAZOLIN (G), CEPHAZOLIN (G)
with stcm as (
	select * 
	from 
		(
			select *,
			regexp_replace(source_code, '\s*\(.*$', '', 'g') as "base_source_code",
			row_number() over (partition by source_code, target_concept_id order by source_code) as row_num
			from {OMOP_SCHEMA}.source_to_concept_map
			where source_vocabulary_id = 'SG_PASAR_INTRAOP_DRUG_MED'
		) temp
	where row_num = 1 -- 81 records
),

dm as (
	select 
		id,
		anon_case_no,
		session_id,
		medication_startdate as "drug_exposure_start_date",
		medication_startdatetime as "drug_exposure_start_datetime",
		operation_enddate as "drug_exposure_end_date",
		(operation_enddate::text || ' ' || operation_endtime)::timestamp as "drug_exposure_end_datetime",
		dosage_individual as "quantity",
		medication_name as "medication_name",
		drug_name as "drug_name",
		case 
			when right(upper(medication_name), 2) = '~T' then upper(left(medication_name, -2)) -- 65 records
			else upper(medication_name)
		end
			as "standardized_medication_name",
		
		'{INTRAOP_SCHEMA}' as "source_schema",
		'drugmed' as "source_table"
	from {INTRAOP_SCHEMA}.drugmed dm 
	where medication_name is not null -- 16909 records
),


dm_stcm_join as (
	select * 
	from dm
	left join stcm
	on stcm.source_code = dm.standardized_medication_name --17140 records
),

-- Joined table with matching target_concept_id
dm_stcm_join_match as (
	select * 
	from dm_stcm_join 
	where target_concept_id is not null -- 16669 records
),

-- Attempt to match again using base_medication_name = base_source_code
dm_stcm_join_no_match as (
	select *,
	regexp_replace(standardized_medication_name, '\s*\(.*$', '', 'g') as "base_medication_name"
	from dm_stcm_join where target_concept_id is null -- 472 records
),

dm_stcm_join_base_match as (
	select 
		row_number() over (partition by dm_stcm_join_no_match.id order by stcm.target_concept_id) as "unique_rn_per_id",
		stcm.target_concept_id as "new_target_concept_id",
		dm_stcm_join_no_match.*
	from dm_stcm_join_no_match
	left join stcm
	on dm_stcm_join_no_match.base_medication_name = stcm.base_source_code -- 508 records
),

--Union
final_dm_stcm as (
	select
		id,
		anon_case_no,
		session_id,
		drug_exposure_start_date,
		drug_exposure_start_datetime,
		drug_exposure_end_date,
		drug_exposure_end_datetime,
		quantity,
		medication_name as "drug_source_value",
		target_concept_id as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_join_match -- 16668 records
	
	union all
	
	select 
		id,
		anon_case_no,
		session_id,
		drug_exposure_start_date,
		drug_exposure_start_datetime,
		drug_exposure_end_date,
		drug_exposure_end_datetime,
		quantity,
		medication_name as "drug_source_value",
		case when new_target_concept_id is null then 0 else new_target_concept_id end as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_join_base_match 
	where dm_stcm_join_base_match.unique_rn_per_id = 1 -- 472 records
	
)

select * from final_dm_stcm --17140 records


-- Notes for duplicated data
--select count(*) from dm where standardized_medication_name = 'AUGMENTIN (GM)' --118 records -> 236 records
--select count(*) from dm where standardized_medication_name = 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)' --3 records -> 6 records
--select count(*) from dm where standardized_medication_name = 'PROPOFOL/LIGNOCAINE (MG)' --80 records -> 160 records
--select count(*) from dm where standardized_medication_name = 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)' --30 records -> 60 records
--select count(*) from dm where standardized_medication_name = 'CEFAZOLIN (G)' --527 records -> 527 records
--select count(*) from dm where standardized_medication_name = 'CEPHAZOLIN (G)' --17 records -> 17 records
