-- *******************************************************************
-- NAME: stg__drug_exposure.sql
-- Create temp staging view before data load that brings in person_id, drug_type_concept_id
-- *******************************************************************
-- Todo:
-- Create drug_exposure_id
-- Provider_id, visit_occurrence_id
-- *******************************************************************


-- Create the staging view for the person table, assigning a unique person_id
create or replace view {OMOP_SCHEMA}.stg__drug_exposure AS
    -- Extract relevant columns from the pre_op.char table
select
	ROW_NUMBER() OVER (ORDER BY drug_exp_view.id, drug_exp_view.drug_exposure_start_datetime) as drug_exposure_id,
	p.person_id as person_id,
	0 as drug_concept_id, --Todo: placeholder
	drug_exp_view.drug_exposure_start_date,
	drug_exp_view.drug_exposure_start_datetime,
	drug_exp_view.drug_exposure_end_date,
	drug_exp_view.drug_exposure_end_datetime,
	32879 as drug_type_concept_id, --Todo: placeholder
	drug_exp_view.quantity,
	case when quantity > 0 then 1 else 0 end as days_supply, -- Todo: need to clarify logic
	drug_exp_view.drug_source_value,
	drug_exp_view.dose_unit_source_value
from {OMOP_SCHEMA}.temp_drug_exposure_view drug_exp_view
left join {OMOP_SCHEMA}.person p
on p.person_source_value = drug_exp_view.anon_case_no

