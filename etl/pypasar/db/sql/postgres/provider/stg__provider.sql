-- *******************************************************************
-- NAME: stg__provider.sql
-- DESC: Create the staging view - provider
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-08-27  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the provider table, assigning a unique provider_id
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__provider AS
    -- Extract relevant columns from the pre_op.operation table
    WITH unique_surgeons AS (
    SELECT surgeon_table.anon_surgeon_name AS name, surgeon_table.surgical_specialty AS specialty
    FROM (
      SELECT anon_surgeon_name, surgical_specialty, count(*), ROW_NUMBER() OVER(PARTITION by anon_surgeon_name ORDER BY count(*) DESC) AS rnk
      FROM {PREOP_SCHEMA}."operation"
      WHERE anon_surgeon_name IS NOT NULL
      GROUP by anon_surgeon_name, surgical_specialty
      ORDER BY anon_surgeon_name, count(*) DESC
    ) AS surgeon_table 
    WHERE surgeon_table.rnk = 1
  ),
  unique_anaesthetists_1 AS (
    SELECT anaesthetist_1_table.anon_plan_anaesthetist_1_name AS name, anaesthetist_1_table.plan_anaesthetist_1_type AS specialty
    FROM (
      SELECT anon_plan_anaesthetist_1_name, plan_anaesthetist_1_type, count(*), ROW_NUMBER() OVER(PARTITION by anon_plan_anaesthetist_1_name ORDER BY count(*) DESC) AS rnk
      FROM {PREOP_SCHEMA}."operation"
      WHERE anon_plan_anaesthetist_1_name IS NOT NULL
      GROUP by anon_plan_anaesthetist_1_name, plan_anaesthetist_1_type
      ORDER BY anon_plan_anaesthetist_1_name, count(*) DESC
    ) AS anaesthetist_1_table 
    WHERE anaesthetist_1_table.rnk = 1
  ),
  unique_anaesthetists_2 AS (
    SELECT anaesthetist_2_table.anon_plan_anaesthetist_2_name AS name, anaesthetist_2_table.plan_anaesthetist_2_type AS specialty
    FROM (
      SELECT anon_plan_anaesthetist_2_name, plan_anaesthetist_2_type, count(*), ROW_NUMBER() OVER(PARTITION by anon_plan_anaesthetist_2_name ORDER BY count(*) DESC) AS rnk
      FROM {PREOP_SCHEMA}."operation"
      WHERE anon_plan_anaesthetist_2_name IS NOT NULL
      GROUP by anon_plan_anaesthetist_2_name, plan_anaesthetist_2_type
      ORDER BY anon_plan_anaesthetist_2_name, count(*) DESC
    ) AS anaesthetist_2_table 
    WHERE anaesthetist_2_table.rnk = 1
  )

  SELECT  ROW_NUMBER() OVER (ORDER BY specialty ASC) AS provider_id,
          * 
  FROM
  (
    SELECT * FROM unique_surgeons
    UNION ALL
    SELECT anaesthetists_1.name, anaesthetists_1.specialty 
    FROM unique_anaesthetists_1 AS anaesthetists_1
    LEFT JOIN unique_anaesthetists_2 AS anaesthetists_2
    ON anaesthetists_1.name = anaesthetists_2.name AND anaesthetists_2.name is NULL
    UNION ALL
    SELECT anaesthetists_2.name, anaesthetists_2.specialty 
    FROM unique_anaesthetists_1 AS anaesthetists_1
    RIGHT JOIN unique_anaesthetists_2 AS anaesthetists_2
    ON anaesthetists_1.name = anaesthetists_2.name AND anaesthetists_1.name is NULL
  ) AS final
  ORDER BY specialty ASC;