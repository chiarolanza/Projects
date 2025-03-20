CREATE DATABASE IF NOT EXISTS healthcare;

-- Tabela de Patients
CREATE TABLE healthcare.patients
(
    patient_id       UUID DEFAULT generateUUIDv4(),
    SSN              Nullable(String) DEFAULT NULL,
    full_name        String,
    gender           String,
    phone_number     Nullable(String) DEFAULT NULL,
    address_line     Nullable(String) DEFAULT NULL,
    city             Nullable(String) DEFAULT NULL,
    state            Nullable(String) DEFAULT NULL,
    country          Nullable(String) DEFAULT NULL,
    postal_code      Nullable(String) DEFAULT NULL,
    created_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY patient_id;

ALTER TABLE healthcare.patients
    ADD INDEX idx_gender (gender) TYPE bloom_filter(0.01) GRANULARITY 3;

-- Tabela de Conditions
CREATE TABLE healthcare.conditions
(
    condition_id         UUID DEFAULT generateUUIDv4(),
    patient_ref          UUID,
    condition_description String,
    condition_status     String,
    created_at           DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY condition_id;

ALTER TABLE healthcare.conditions
    ADD INDEX idx_condition_description (condition_description) TYPE bloom_filter(0.01) GRANULARITY 3;

-- Tabela de Medications
CREATE TABLE healthcare.medications
(
    med_id               UUID DEFAULT generateUUIDv4(),
    patient_ref          UUID,
    medication_name      String,
    dosage               Nullable(String) DEFAULT NULL,
    administration_route String,
    prescribing_doctor   Nullable(String) DEFAULT NULL,
    prescription_status  String,
    frequency            Nullable(String) DEFAULT NULL,
    created_at           DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY med_id;

ALTER TABLE healthcare.medications
    ADD INDEX idx_medication_name (medication_name) TYPE bloom_filter(0.01) GRANULARITY 3;