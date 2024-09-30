-- *******************************************************************
-- NAME: stg__specimen.sql
-- DESC: Create the staging view - specimen
-- *******************************************************************
-- CHANGE LOG:
-- DATE        VERS  INITIAL  CHANGE DESCRIPTION
-- ----------  ----  -------  ----------------------------------------
-- 2024-09-30  1.00           Initial create
-- *******************************************************************

-- Create the staging view for the specimen table
CREATE OR REPLACE VIEW {OMOP_SCHEMA}.stg__specimen AS
    -- Extract relevant columns from the post_op.lab_micro table
    WITH postop__labmicro AS (
        SELECT
            id AS specimen_source_value,
            specimen_collection_date,
            specimen_collection_time,
            anon_case_no,
            micro_resulted_procedure_description	
        FROM {POSTOP_SCHEMA}.labmicro
    ),
    -- Join postop__labmicro with person for person_id
    joining AS (
        SELECT 
            l.*,
            p.person_id AS person_id
        FROM postop__labmicro AS l
        JOIN {OMOP_SCHEMA}.person AS p
            ON l.anon_case_no = p.person_source_value
    )
    SELECT
        ROW_NUMBER() OVER(ORDER BY specimen_source_value, specimen_collection_date, specimen_collection_time) AS specimen_id,
        person_id,
        micro_resulted_procedure_description,   -- TODO: For mapping with the specimen_concept_id field
        specimen_collection_date AS specimen_date,
        (CONCAT(specimen_collection_date, ' ', specimen_collection_time))::timestamp AS specimen_datetime,
        specimen_source_value
    FROM joining;