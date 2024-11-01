-- ***********************************************************************
-- NAME: temp_drug_fluids_view.sql
-- Create temp view that joins intraop.drugfluids with source_to_concept_map
-- ***********************************************************************


create or replace view {OMOP_SCHEMA}.{DRUGFLUIDS_STCM_VIEW} as

select
	id,
	anon_case_no,
	session_id,
	coalesce(fluid_startdate, operation_startdate)::date as "drug_exposure_start_date",
	coalesce(
			(fluid_startdate:: text || ' ' || fluid_starttime)::timestamp,
			(operation_startdate::text || ' ' || operation_starttime)::timestamp
		) as "drug_exposure_start_datetime",
	coalesce(fluid_enddate , operation_enddate)::date as "drug_exposure_end_date",
	coalesce(
			(fluid_enddate:: text || ' ' || fluid_endtime)::timestamp,
			(operation_enddate::text || ' ' || operation_endtime)::timestamp
		) as "drug_exposure_end_datetime",
	fluid_volume_actual as "quanity",
	fluid_name as "drug_source_value",
	0 as "drug_concept_id",
	'{INTRAOP_SCHEMA}' as "source_schema",
	'drugfluids' as "source_table"
from {INTRAOP_SCHEMA}.drugfluids df
where fluid_name is not null -- 6505 records