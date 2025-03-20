DROP TABLE IF EXISTS healthcare.mv_common_conditions;

CREATE MATERIALIZED VIEW healthcare.mv_common_conditions
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(min_created_at)
ORDER BY condition_description
POPULATE AS
SELECT 
    condition_description,
    countState() AS cnt_state,
    min(created_at) AS min_created_at
FROM healthcare.conditions
GROUP BY condition_description;

DROP TABLE IF EXISTS healthcare.mv_male_patients;

CREATE MATERIALIZED VIEW healthcare.mv_male_patients
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY tuple()
POPULATE AS
SELECT 1 AS count_value, created_at
FROM healthcare.patients
WHERE gender = 'Male';

DROP TABLE IF EXISTS healthcare.mv_common_medications;

CREATE MATERIALIZED VIEW healthcare.mv_common_medications
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(min_created_at)
ORDER BY medication_name
POPULATE AS
SELECT 
    medication_name,
    countState() AS cnt_state,
    min(created_at) AS min_created_at
FROM healthcare.medications
GROUP BY medication_name;

SELECT condition_description, finalizeAggregation(cnt_state) AS cnt
FROM healthcare.mv_common_conditions
ORDER BY cnt DESC
LIMIT 10;

SELECT medication_name, finalizeAggregation(cnt_state) AS prescriptions
FROM healthcare.mv_common_medications
ORDER BY prescriptions DESC
LIMIT 10;

SELECT sum(count_value) AS male_patients_count
FROM healthcare.mv_male_patients;
