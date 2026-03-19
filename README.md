## 🧊 Projet : Analyse des Offres LinkedIn avec Snowflake

### 📌 Pipeline complet : ingestion → structuration → préparation des analyses

```sql
-- ============================================================================
-- 00 - INITIALISATION DE L’ENVIRONNEMENT
-- Objectif : créer la base et organiser les schémas
-- ============================================================================

CREATE OR REPLACE DATABASE LINKEDIN;

-- Organisation en couches (bonne pratique data engineering)
CREATE OR REPLACE SCHEMA LINKEDIN.RAW;   -- données brutes
CREATE OR REPLACE SCHEMA LINKEDIN.CORE;  -- données transformées
CREATE OR REPLACE SCHEMA LINKEDIN.MART;  -- données analytiques

-- Positionnement dans le bon contexte
USE DATABASE LINKEDIN;
USE SCHEMA RAW;


-- ============================================================================
-- 01 - CREATION DU STAGE S3
-- Objectif : connecter Snowflake au bucket public
-- ============================================================================

CREATE OR REPLACE STAGE linkedin_s3_stage
  URL = 's3://snowflake-lab-bucket/';

-- Vérification du contenu du bucket
LIST @linkedin_s3_stage;


-- ============================================================================
-- 02 - CREATION DES FORMATS DE FICHIERS
-- Objectif : définir comment lire CSV et JSON
-- ============================================================================

-- Format CSV
CREATE OR REPLACE FILE FORMAT ff_csv_linkedin
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

-- Format JSON
CREATE OR REPLACE FILE FORMAT ff_json_linkedin
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;


-- ============================================================================
-- 03 - CREATION DES TABLES RAW
-- Objectif : stocker les données brutes
-- ============================================================================

-- Table des avantages
CREATE OR REPLACE TABLE benefits_raw (
    job_id NUMBER,
    inferred BOOLEAN,
    type STRING
);

-- Table des effectifs
CREATE OR REPLACE TABLE employee_counts_raw (
    company_id NUMBER,
    employee_count NUMBER,
    follower_count NUMBER,
    time_recorded NUMBER
);

-- Table principale des offres d’emploi
CREATE OR REPLACE TABLE job_postings_raw (
    job_id NUMBER,
    company_id NUMBER,
    title STRING,
    description STRING,
    max_salary FLOAT,
    med_salary FLOAT,
    min_salary FLOAT,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies NUMBER,
    original_listed_time NUMBER,
    remote_allowed BOOLEAN,
    views NUMBER,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry NUMBER,
    closed_time NUMBER,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time NUMBER,
    posting_domain STRING,
    sponsored BOOLEAN,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- Table des compétences
CREATE OR REPLACE TABLE job_skills_raw (
    job_id NUMBER,
    skill_abr STRING
);

-- Tables JSON (semi-structurées)
CREATE OR REPLACE TABLE companies_json_raw (src VARIANT);
CREATE OR REPLACE TABLE company_industries_json_raw (src VARIANT);
CREATE OR REPLACE TABLE company_specialities_json_raw (src VARIANT);
CREATE OR REPLACE TABLE job_industries_json_raw (src VARIANT);


-- ============================================================================
-- 04 - CHARGEMENT DES DONNEES
-- Objectif : ingestion depuis S3
-- ============================================================================

-- Nettoyage avant chargement
TRUNCATE TABLE benefits_raw;
TRUNCATE TABLE employee_counts_raw;
TRUNCATE TABLE job_postings_raw;
TRUNCATE TABLE job_skills_raw;
TRUNCATE TABLE companies_json_raw;
TRUNCATE TABLE company_industries_json_raw;
TRUNCATE TABLE company_specialities_json_raw;
TRUNCATE TABLE job_industries_json_raw;

-- ================= CSV =================

COPY INTO benefits_raw
FROM @linkedin_s3_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = ff_csv_linkedin)
ON_ERROR = CONTINUE;

COPY INTO employee_counts_raw
FROM @linkedin_s3_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = ff_csv_linkedin)
ON_ERROR = CONTINUE;

COPY INTO job_postings_raw
FROM @linkedin_s3_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = ff_csv_linkedin)
ON_ERROR = CONTINUE;

COPY INTO job_skills_raw
FROM @linkedin_s3_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = ff_csv_linkedin)
ON_ERROR = CONTINUE;

-- ================= JSON =================

COPY INTO companies_json_raw
FROM @linkedin_s3_stage/companies.json
FILE_FORMAT = (FORMAT_NAME = ff_json_linkedin)
ON_ERROR = CONTINUE;

COPY INTO company_industries_json_raw
FROM @linkedin_s3_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = ff_json_linkedin)
ON_ERROR = CONTINUE;

COPY INTO company_specialities_json_raw
FROM @linkedin_s3_stage/company_specialities.json
FILE_FORMAT = (FORMAT_NAME = ff_json_linkedin)
ON_ERROR = CONTINUE;

COPY INTO job_industries_json_raw
FROM @linkedin_s3_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = ff_json_linkedin)
ON_ERROR = CONTINUE;


-- ============================================================================
-- 05 - RESULTAT
-- ============================================================================

-- A ce stade :
-- ✔ Données CSV → tables structurées
-- ✔ Données JSON → colonnes VARIANT
-- ✔ Pipeline prête pour transformation (CORE)
-- ✔ Processus automatisable et reproductible
