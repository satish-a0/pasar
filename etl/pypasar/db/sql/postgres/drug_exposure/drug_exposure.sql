INSERT INTO {OMOP_SCHEMA}.{DRUG_EXPOSURE_TABLE}
(
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    drug_type_concept_id,
    quantity,
    days_supply,
    visit_occurrence_id,
    drug_source_value
)
SELECT
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    drug_type_concept_id,
    quantity,
    days_supply,
    visit_occurrence_id,
    drug_source_value
FROM {OMOP_SCHEMA}.{DRUG_EXPOSURE_STG_VIEW};