WITH
-- Extract drug exposure info and calculates end dates
ctePreDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS (
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date,
        d.days_supply,
        COALESCE(
            -- If drug_exposure_end_date is not NULL, return it
            drug_exposure_end_date,
            -- If days_supply is not NULL or 0, return drug_exposure_start_date + days_supply
            CASE 
                WHEN days_supply IS NOT NULL AND days_supply > 0 THEN drug_exposure_start_date + days_supply * INTERVAL '1 day'
                ELSE NULL
            END,
            -- Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
            drug_exposure_start_date + INTERVAL '1 day'
        ) AS drug_exposure_end_date
    FROM {OMOP_SCHEMA}.drug_exposure d
    JOIN {OMOP_SCHEMA}.concept_ancestor ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    JOIN {OMOP_SCHEMA}.concept c
        ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
        AND c.concept_class_id = 'Ingredient'
        AND d.drug_concept_id != 0
        AND COALESCE(d.days_supply, 0) >= 0
),

-- Identify end dates for overlapping drug exposures
cteSubExposureEndDates(person_id, ingredient_concept_id, end_date) AS (
    SELECT person_id, ingredient_concept_id, event_date AS end_date
    FROM (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
            MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_exposure_start_date AS event_date, -1 AS event_type,
                ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
            FROM ctePreDrugTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, drug_exposure_end_date, 1 AS event_type, NULL
            FROM ctePreDrugTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- Aggregate end dates for each person and drug
cteDrugExposureEnds(person_id, drug_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date) AS (
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget dt
    JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date
),

-- Group sub-exposures, counting occurrences and dates
cteSubExposures(row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count) AS (
    SELECT ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_date ORDER BY person_id) AS row_number,
        person_id, drug_concept_id, MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date, drug_sub_exposure_end_date, COUNT(*) AS drug_exposure_count
    FROM cteDrugExposureEnds
    GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_date
),

-- Compute drug exposure durations
cteFinalTarget(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count, days_exposed) AS (
    SELECT row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count,
        DATEDIFF(DAY, drug_sub_exposure_start_date, drug_sub_exposure_end_date) AS days_exposed
    FROM cteSubExposures
),


-- Establish potential end dates for exposures
cteEndDates(person_id, ingredient_concept_id, end_date) AS (
    SELECT person_id, ingredient_concept_id, DATEADD(DAY, -30, event_date) AS end_date
    FROM (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
            MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_sub_exposure_start_date AS event_date, -1 AS event_type,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
            FROM cteFinalTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, DATEADD(DAY, 30, drug_sub_exposure_end_date), 1 AS event_type
            FROM cteFinalTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- Define drug eras, consolidating counts and exposure days
cteDrugEraEnds(person_id, drug_concept_id, drug_sub_exposure_start_date, drug_era_end_date, drug_exposure_count, days_exposed) AS (
    SELECT
        ft.person_id,
        ft.ingredient_concept_id,
        ft.drug_sub_exposure_start_date,
        MIN(e.end_date) AS era_end_date,
        drug_exposure_count,
        days_exposed
    FROM cteFinalTarget ft
    JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
    GROUP BY
        ft.person_id,
        ft.ingredient_concept_id,
        ft.drug_sub_exposure_start_date,
        drug_exposure_count,
        days_exposed
)

SELECT
    ROW_NUMBER() OVER (ORDER BY person_id) AS drug_era_id,
    person_id,
    drug_concept_id,
    MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
    drug_era_end_date,
    SUM(drug_exposure_count) AS drug_exposure_count,
    DATEDIFF(DAY, MIN(drug_sub_exposure_start_date), drug_era_end_date) - SUM(days_exposed) AS gap_days
INTO tmp_de
FROM cteDrugEraEnds dee
GROUP BY person_id, drug_concept_id, drug_era_end_date;

INSERT INTO {OMOP_SCHEMA}.drug_era(drug_era_id, person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
SELECT * FROM tmp_de;