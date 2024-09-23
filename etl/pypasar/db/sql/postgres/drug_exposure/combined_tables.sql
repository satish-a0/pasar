-- *******************************************************************
-- NAME: temp_drug_exposure_view.sql
-- Create temp view that combines values from all source tables
-- *******************************************************************
-- Todo:
-- Filter out records with quantity null or 0?
-- Drug source values for postop tables not found in mapping
-- *******************************************************************


create or replace view {OMOP_SCHEMA}.temp_drug_exposure_view as

with 
io_dd as (
	select
		dd.id,
		dd.anon_case_no_clindoc,
		dd.anon_case_no,
		dd.session_id,
		dd.operation_id,
		coalesce(dd.infusion_startdatetime, dd.operation_startdate)::timestamp as drug_exposure_start_date,
		coalesce(
			dd.infusion_startdatetime,
			(dd.operation_startdate::text || ' ' || dd.operation_starttime)::timestamp
		) as drug_exposure_start_datetime,
		dd.operation_enddate as drug_exposure_end_date,
		(dd.operation_enddate::text || ' ' || dd.operation_endtime)::timestamp as drug_exposure_end_datetime,
		dd.volume * dd.concentration as quantity, -- or dose?
		dd.drug_name as drug_source_value,
		null as dose_unit_source_value,
		'io_dd' as source_table
	from {INTRAOP_SCHEMA}.drugdrug dd
),

io_dm as (

	select 
		dm.id,
		dm.anon_case_no_clindoc,
		dm.anon_case_no,
		dm.session_id,
		dm.operation_id,
		dm.medication_startdate as drug_exposure_start_date,
		dm.medication_startdatetime as drug_exposure_start_datetime,
		dm.operation_enddate as drug_exposure_end_date,
		(dm.operation_enddate::text || ' ' || dm.operation_endtime)::timestamp as drug_exposure_end_datetime,
		dm.dosage_individual as quantity,
		dm.medication_name as drug_source_value,
		dm.medication_name as dose_unit_source_value,
		'io_dm' as source_table
		
	from {INTRAOP_SCHEMA}.drugmed dm
	-- where -- any filter conditions
),

io_df as (

	select 
		df.id,
		df.anon_case_no_clindoc,
		df.anon_case_no,
		df.session_id,
		df.operation_id,
		df.fluid_startdate as drug_exposure_start_date,
		(df.fluid_startdate::text || ' ' || df.fluid_starttime)::timestamp as drug_exposure_start_datetime,
		coalesce(df.fluid_enddate, df.operation_enddate) as drug_exposure_end_date,
		coalesce(
			df.fluid_enddate::text || ' ' || df.fluid_endtime,
			df.operation_enddate::text || ' ' || df.operation_endtime
			)::timestamp as drug_exposure_end_datetime,
		df.fluid_volume_actual as quantity,
		df.drug_name as drug_source_value,
		df.fluid_name as dose_unit_source_value,
		'io_df' as source_table
	from {INTRAOP_SCHEMA}.drugfluids df
	-- where -- filter out urine and bloodloss
), 

io_db as (
	select
		db.id,
		db.anon_case_no_clindoc,
		db.anon_case_no,
		db.session_id,
		db.operation_id,
		db.operation_startdate as drug_exposure_start_date,
		(db.operation_startdate::text || ' ' || db.operation_starttime)::timestamp as drug_exposure_start_datetime,
		db.operation_enddate as drug_exposure_end_date,
		(db.operation_enddate::text || ' ' || db.operation_endtime)::timestamp as drug_exposure_end_datetime,
		0 as quantity, -- Todo: placeholder
		null as drug_source_value, -- Todo: placeholder
		null as dose_unit_source_value,
		'io_db' as source_table
	from {INTRAOP_SCHEMA}.drugblocks db
	-- where -- filter out urine and bloodloss
),

po_v_adrenaline as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		Adrenaline_Dose_Outcome as quantity,
		'Adrenaline_Dose_Outcome' as drug_source_value, -- Todo: placeholder
		pv.Adrenaline_Dose_Outcome::text as dose_unit_source_value,
		'po_v_adrenaline' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Adrenaline_Dose_Outcome is not null
), 

po_v_amiodarone as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		Amiodarone_Dose_mg_hr as quantity,
		'Amiodarone_Dose_mg_hr' as drug_source_value, -- Todo: placeholder
		pv.Amiodarone_Dose_mg_hr::text as dose_unit_source_value,
		'po_v_amiodarone' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Amiodarone_Dose_mg_hr is not null
),

po_v_dobutamine as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(Dobutamine_Dose_mcg_kg_min_Outcome::numeric, 0) as quantity,
		'Dobutamine_Dose_mcg_kg_min_Outcome' as drug_source_value, -- Todo: placeholder
		pv.Dobutamine_Dose_mcg_kg_min_Outcome::text as dose_unit_source_value,
		'po_v_dobutamine' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Dobutamine_Dose_mcg_kg_min_Outcome is not null
),

po_v_glyceryl_trinitrate as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(Glyceryl_Trinitrate_Dose_mcg_min::numeric, 0) as quantity,
		'Glyceryl_Trinitrate_Dose_mcg_min' as drug_source_value, -- Todo: placeholder
		pv.Glyceryl_Trinitrate_Dose_mcg_min::text as dose_unit_source_value,
		'po_v_glyceryl_trinitrate' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Glyceryl_Trinitrate_Dose_mcg_min is not null
),

po_v_vasopressin as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(Vasopressin_Dose_unit_hr_Outcome, 0) as quantity,
		'Vasopressin_Dose_unit_hr_Outcome' as drug_source_value, -- Todo: placeholder
		pv.Vasopressin_Dose_unit_hr_Outcome::text as dose_unit_source_value,
		'po_v_vasopressin' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Vasopressin_Dose_unit_hr_Outcome is not null
),

po_v_noradrenaline as (
	select
		pv.id,
		pv.anon_case_no_clindoc,
		pv.anon_case_no,
		pv.session_id,
		pv.operation_id,
		pv.operation_startdate as drug_exposure_start_date,
		(pv.operation_startdate::text || ' ' || pv.operation_starttime)::timestamp as drug_exposure_start_datetime,
		pv.operation_enddate as drug_exposure_end_date,
		(pv.operation_enddate::text || ' ' || pv.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(Noradrenaline_Dose_mcg_kg_min, 0) as quantity,
		'Noradrenaline_Dose_mcg_kg_min' as drug_source_value, -- Todo: placeholder
		pv.Noradrenaline_Dose_mcg_kg_min::text as dose_unit_source_value,
		'po_v_noradrenaline' as source_table
	from {POSTOP_SCHEMA}.vasoactives pv
	where Noradrenaline_Dose_mcg_kg_min is not null
),

po_icu_adrenaline_epinephrine as (
	select
		po_icu.id,
		po_icu.anon_case_no_clindoc,
		po_icu.anon_case_no,
		po_icu.session_id,
		po_icu.operation_id,
		po_icu.operation_startdate as drug_exposure_start_date,
		(po_icu.operation_startdate::text || ' ' || po_icu.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_icu.operation_enddate as drug_exposure_end_date,
		(po_icu.operation_enddate::text || ' ' || po_icu.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(po_icu.Adrenaline_Epinephrine_Dose_mcg_kg_min, 0) as quantity,
		'Adrenaline_Epinephrine_Dose_mcg_kg_min' as drug_source_value, -- Todo: placeholder
		po_icu.Adrenaline_Epinephrine_Dose_mcg_kg_min::text as dose_unit_source_value,
		'po_icu_adrenaline_epinephrine' as source_table
	from {POSTOP_SCHEMA}.icu po_icu
	where po_icu.Adrenaline_Epinephrine_Dose_mcg_kg_min is not null
),

po_icu_dobutamine as (
	select
		po_icu.id,
		po_icu.anon_case_no_clindoc,
		po_icu.anon_case_no,
		po_icu.session_id,
		po_icu.operation_id,
		po_icu.operation_startdate as drug_exposure_start_date,
		(po_icu.operation_startdate::text || ' ' || po_icu.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_icu.operation_enddate as drug_exposure_end_date,
		(po_icu.operation_enddate::text || ' ' || po_icu.operation_endtime)::timestamp as drug_exposure_end_datetime,
		coalesce(po_icu.Dobutamine_Dose_mcg_kg_min::numeric, 0) as quantity,
		'Dobutamine_Dose_mcg_kg_min' as drug_source_value, -- Todo: placeholder
		po_icu.Dobutamine_Dose_mcg_kg_min::text as dose_unit_source_value,
		'po_icu_adrenaline_dobutamine' as source_table
	from {POSTOP_SCHEMA}.icu po_icu
	where po_icu.Dobutamine_Dose_mcg_kg_min is not null
),

po_icu_dopamine as (
	select
		po_icu.id,
		po_icu.anon_case_no_clindoc,
		po_icu.anon_case_no,
		po_icu.session_id,
		po_icu.operation_id,
		po_icu.operation_startdate as drug_exposure_start_date,
		(po_icu.operation_startdate::text || ' ' || po_icu.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_icu.operation_enddate as drug_exposure_end_date,
		(po_icu.operation_enddate::text || ' ' || po_icu.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_icu.Dopamine_Dose_mcg_kg_min as quantity,
		'Dopamine_Dose_mcg_kg_min' as drug_source_value, -- Todo: placeholder
		po_icu.Dopamine_Dose_mcg_kg_min::text as dose_unit_source_value,
		'po_icu_dopamine' as source_table
	from {POSTOP_SCHEMA}.icu po_icu
	where po_icu.Dopamine_Dose_mcg_kg_min is not null
),

po_icu_morphine as (
	select
		po_icu.id,
		po_icu.anon_case_no_clindoc,
		po_icu.anon_case_no,
		po_icu.session_id,
		po_icu.operation_id,
		po_icu.operation_startdate as drug_exposure_start_date,
		(po_icu.operation_startdate::text || ' ' || po_icu.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_icu.operation_enddate as drug_exposure_end_date,
		(po_icu.operation_enddate::text || ' ' || po_icu.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_icu.Morphine_Dose_mg_hr as quantity,
		'Morphine_Dose_mg_hr' as drug_source_value, -- Todo: placeholder
		po_icu.Morphine_Dose_mg_hr::text as dose_unit_source_value,
		'po_icu_morphine' as source_table
	from {POSTOP_SCHEMA}.icu po_icu
	where po_icu.Morphine_Dose_mg_hr is not null
),

po_icu_noradrenaline as (
	select
		po_icu.id,
		po_icu.anon_case_no_clindoc,
		po_icu.anon_case_no,
		po_icu.session_id,
		po_icu.operation_id,
		po_icu.operation_startdate as drug_exposure_start_date,
		(po_icu.operation_startdate::text || ' ' || po_icu.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_icu.operation_enddate as drug_exposure_end_date,
		(po_icu.operation_enddate::text || ' ' || po_icu.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_icu.Noradrenaline_Dose_mcg_kg_min as quantity,
		'Noradrenaline_Dose_mcg_kg_min' as drug_source_value, -- Todo: placeholder
		po_icu.Noradrenaline_Dose_mcg_kg_min::text as dose_unit_source_value,
		'po_icu_noradrenaline' as source_table
	from {POSTOP_SCHEMA}.icu po_icu
	where po_icu.Noradrenaline_Dose_mcg_kg_min is not null
),

po_io_hartmann as (
	select
		po_io.id,
		po_io.anon_case_no_clindoc,
		po_io.anon_case_no,
		po_io.session_id,
		po_io.operation_id,
		po_io.operation_startdate as drug_exposure_start_date,
		(po_io.operation_startdate::text || ' ' || po_io.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_io.operation_enddate as drug_exposure_end_date,
		(po_io.operation_enddate::text || ' ' || po_io.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_io.Hartmann_Solution_Infusion as quantity,
		'Hartmann_Solution_Infusion' as drug_source_value, -- Todo: placeholder
		po_io.Hartmann_Solution_Infusion::text as dose_unit_source_value,
		'po_io_hartmann' as source_table
	from {POSTOP_SCHEMA}.intakeoutput po_io
	where po_io.Hartmann_Solution_Infusion is not null
),

po_io_na_cl as (
	select
		po_io.id,
		po_io.anon_case_no_clindoc,
		po_io.anon_case_no,
		po_io.session_id,
		po_io.operation_id,
		po_io.operation_startdate as drug_exposure_start_date,
		(po_io.operation_startdate::text || ' ' || po_io.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_io.operation_enddate as drug_exposure_end_date,
		(po_io.operation_enddate::text || ' ' || po_io.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_io.Sodium_Chloride_0_9_Infusion as quantity,
		'Sodium_Chloride_0_9_Infusion' as drug_source_value, -- Todo: placeholder
		po_io.Sodium_Chloride_0_9_Infusion::text as dose_unit_source_value,
		'po_io_na_cl' as source_table
	from {POSTOP_SCHEMA}.intakeoutput po_io
	where po_io.Sodium_Chloride_0_9_Infusion is not null
),

po_io_dextrose as (
	select
		po_io.id,
		po_io.anon_case_no_clindoc,
		po_io.anon_case_no,
		po_io.session_id,
		po_io.operation_id,
		po_io.operation_startdate as drug_exposure_start_date,
		(po_io.operation_startdate::text || ' ' || po_io.operation_starttime)::timestamp as drug_exposure_start_datetime,
		po_io.operation_enddate as drug_exposure_end_date,
		(po_io.operation_enddate::text || ' ' || po_io.operation_endtime)::timestamp as drug_exposure_end_datetime,
		po_io.Dextrose_5_Sodium_Chloride_0_9_Infusion as quantity,
		'Dextrose_5_Sodium_Chloride_0_9_Infusion' as drug_source_value, -- Todo: placeholder
		po_io.Dextrose_5_Sodium_Chloride_0_9_Infusion::text as dose_unit_source_value,
		'po_io_dextrose' as source_table
	from {POSTOP_SCHEMA}.intakeoutput po_io
	where po_io.Dextrose_5_Sodium_Chloride_0_9_Infusion is not null
)


select * from io_dd
union all
select * from io_dm
union all
select * from io_df
union all
select * from io_db
union all
select * from po_v_adrenaline
union all
select * from po_v_amiodarone
union all
select * from po_v_dobutamine
union all
select * from po_v_glyceryl_trinitrate
union all
select * from po_v_vasopressin
union all
select * from po_v_noradrenaline
union all
select * from po_icu_adrenaline_epinephrine
union all
select * from po_icu_dobutamine
union all
select * from po_icu_dopamine
union all
select * from po_icu_morphine
union all
select * from po_icu_noradrenaline
union all
select * from po_io_hartmann
union all
select * from po_io_na_cl
union all
select * from po_io_dextrose





