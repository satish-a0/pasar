INSERT INTO observation_period.observation_period
(
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
)
SELECT
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
FROM {OMOP_SCHEMA}.stg__observation_period;