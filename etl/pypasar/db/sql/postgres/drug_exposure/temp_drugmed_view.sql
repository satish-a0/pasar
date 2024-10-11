-- ***********************************************************************
-- NAME: temp_drug_med_view.sql
-- Create temp view that joins intraop.drugmed with source_to_concept_map
-- ***********************************************************************


create or replace view {OMOP_SCHEMA}.{DRUGMED_STCM_VIEW} as
with 
dm as (
	select 
		id,
		anon_case_no_clindoc,
		anon_case_no,
		session_id,
		operation_id,
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
		
		'intraop' as "source_schema",
		'drugmed' as "source_table"
	from {INTRAOP_SCHEMA}.drugmed -- 16962 records
	where medication_name is not null -- 53 records
    ), -- 16909 records

stcm as (
	select 
		row_number() over (partition by source_code order by target_concept_id) as "source_code_rn",
		regexp_replace(source_code, '\s*\(.*$', '', 'g') as "base_source_code",
		* 
	from {OMOP_SCHEMA}.source_to_concept_map stcm
	where source_vocabulary_id = 'SG_PASAR_INTRAOP_DRUG_MED' 
    ), -- 83 records


-- Remove records with double entry in the join column of dm for sanity check
dm_non_duplicated as (
	select 
		regexp_replace(standardized_medication_name, '\s*\(.*$', '', 'g') as "base_medication_name",
		* 
	from dm 
	where dm.standardized_medication_name not in ('AUGMENTIN (GM)', 'CEFAZOLIN (G)', 'CEPHAZOLIN (G)', 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)', 'PROPOFOL/LIGNOCAINE (MG)', 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)')
    ), -- 16134 records

-- Remove records with double entry in the join column of stcm for sanity check
stcm_non_duplicated as (
	select *
	from stcm
	where stcm.source_code not in ('AUGMENTIN (GM)', 'CEFAZOLIN (G)', 'CEPHAZOLIN (G)', 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)', 'PROPOFOL/LIGNOCAINE (MG)', 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)') --775 records
	and stcm.source_code_rn = 1 -- ensure unique records
    ), -- 71 records (83-12) each of these source code is duplicated

-- Join dm and stcm tables with no duplicated values using exact value match
dm_stcm_non_duplicated as (
	select 
		stcm_non_duplicated.source_code,
		stcm_non_duplicated.target_concept_id,
		dm_non_duplicated.*
	from dm_non_duplicated
	left join stcm_non_duplicated
		on stcm_non_duplicated.source_code = dm_non_duplicated.standardized_medication_name
    ), -- 16134 records
    
-- Joined table with matching target_concept_id
dm_stcm_non_duplicated_match as (
	select * from dm_stcm_non_duplicated where target_concept_id is not null
    ), -- 15662 records    
    
-- Joined table with no matching target_concept_id
dm_stcm_non_duplicated_no_match as (
	select * from dm_stcm_non_duplicated where target_concept_id is null
    ), -- 472 records
    
-- Attempt to match again using base_medication_name = base_source_code
dm_stcm_non_duplicated_match_2 as (
	select 
		row_number() over (partition by t1.id order by t2.target_concept_id) as "unique_rn_per_id",
		t2.target_concept_id as "new_target_concept_id",
		t1.*
	from dm_stcm_non_duplicated_no_match t1
	left join stcm t2 
	on t1.base_medication_name = t2.base_source_code
),
    

-- Extract records with double entry in the join column of stcm for sanity check (not cefazolin)
-- Same source code and different target_concept_id
dm_duplicated_non_cefazolin as (
	select *
	from dm
	where dm.standardized_medication_name in ('AUGMENTIN (GM)', 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)', 'PROPOFOL/LIGNOCAINE (MG)', 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)')
    ), -- 231 records

stcm_duplicated_non_cefazolin as (
	select *
	from stcm
	where stcm.source_code in ('AUGMENTIN (GM)', 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)', 'PROPOFOL/LIGNOCAINE (MG)', 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)')
    ), -- 8 records

dm_stcm_duplicated_non_cefazolin as (
	select 
		stcm_duplicated_non_cefazolin.source_code,
		stcm_duplicated_non_cefazolin.target_concept_id,
		dm_duplicated_non_cefazolin.*
	from dm_duplicated_non_cefazolin
	left join stcm_duplicated_non_cefazolin
		on stcm_duplicated_non_cefazolin.source_code = dm_duplicated_non_cefazolin.standardized_medication_name
), -- 462 records


-- Extract records with double entry in the join column of stcm for sanity check (cefazolin)
-- Same source code and same target_concept_id
dm_duplicated_cefazolin as (
	select *
	from dm
	where dm.standardized_medication_name in ('CEFAZOLIN (G)', 'CEPHAZOLIN (G)')
    ), -- 544 records

stcm_duplicated_cefazolin as (
	select *
	from stcm
	where stcm.source_code in ( 'CEFAZOLIN (G)', 'CEPHAZOLIN (G)')
	and source_code_rn=1 -- remove duplicated records
    ), -- 2 records

dm_stcm_duplicated_cefazolin as (
	select 
		stcm_duplicated_cefazolin.source_code,
		stcm_duplicated_cefazolin.target_concept_id,
		dm_duplicated_cefazolin.*
	from dm_duplicated_cefazolin
	left join stcm_duplicated_cefazolin
		on stcm_duplicated_cefazolin.source_code = dm_duplicated_cefazolin.standardized_medication_name
    ), -- 544 records


-- Union
final_dm_stcm as (
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
		medication_name as "drug_source_value",
		target_concept_id as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_non_duplicated_match -- 15662 records
	
	union all
	
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
		medication_name as "drug_source_value",
		case when new_target_concept_id is null then 0 else new_target_concept_id end as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_non_duplicated_match_2 
	where dm_stcm_non_duplicated_match_2.unique_rn_per_id = 1 -- 472 records
	
	union all
	
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
		medication_name as "drug_source_value",
		target_concept_id as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_duplicated_non_cefazolin -- 462 records
	
	union all
	
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
		medication_name as "drug_source_value",
		target_concept_id as "drug_concept_id",
		source_schema,
		source_table
	from dm_stcm_duplicated_cefazolin -- 544 records
) 

select * from final_dm_stcm -- Total: 17140 records (16909 + 118 + 3 + 80 + 30 with double concept ids)


--select count(*) from dm where standardized_medication_name = 'AUGMENTIN (GM)' --118 records -> 236 records
--select count(*) from dm where standardized_medication_name = 'BUPIVACAINE 0.5% WITH DEXTROSE (MG)' --3 records -> 6 records
--select count(*) from dm where standardized_medication_name = 'PROPOFOL/LIGNOCAINE (MG)' --80 records -> 160 records
--select count(*) from dm where standardized_medication_name = 'TAZOCIN-PIPERACILLIN,TAZOBACTAM (G)' --30 records -> 60 records
--select count(*) from dm where standardized_medication_name = 'CEFAZOLIN (G)' --527 records -> 527 records
--select count(*) from dm where standardized_medication_name = 'CEPHAZOLIN (G)' --17 records -> 17 records
