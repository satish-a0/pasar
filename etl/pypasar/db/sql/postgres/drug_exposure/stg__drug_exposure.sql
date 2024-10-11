-- *******************************************************************
-- NAME: stg__drug_exposure.sql
-- Create temp staging view before data load that brings in person_id, drug_type_concept_id
-- *******************************************************************
-- Todo:
-- Create drug_exposure_id
-- Provider_id, visit_occurrence_id
-- *******************************************************************


-- Create the staging view for the person table, assigning a unique person_id
create or replace view {OMOP_SCHEMA}.{DRUG_EXPOSURE_STG_VIEW} AS

with combined as (
	select * from {OMOP_SCHEMA}.{DRUGMED_STCM_VIEW} -- 17140 records
	union all
	select * from {OMOP_SCHEMA}.{DRUGDRUG_STCM_VIEW} -- 2344 records
	-- union all
	-- select * from {OMOP_SCHEMA}.temp_drug_fluids_view
)

select
	ROW_NUMBER() OVER (ORDER BY combined.id, combined.drug_exposure_start_datetime) as drug_exposure_id,
	p.person_id as "person_id",
	combined.drug_concept_id as "drug_concept_id",
	combined.drug_exposure_start_date as "drug_exposure_start_date",
	combined.drug_exposure_start_datetime as "drug_exposure_start_datetime",
	combined.drug_exposure_end_date as "drug_exposure_end_date",
	combined.drug_exposure_end_datetime as "drug_exposure_end_datetime",
	32879 as "drug_type_concept_id", -- source is registry
	combined.quantity as "quantity",
	case when combined.source_schema = 'intraop' then 1 else 0 end as "days_supply",
	combined.drug_source_value as "drug_source_value"
from combined
left join {OMOP_SCHEMA}.person p
on p.person_source_value = combined.anon_case_no

--left join {OMOP_SCHEMA}.visit_occurrence vo
-- on...