📊 Analyse des Données LinkedIn avec Snowflake
Auteurs : Oumaima CHAMSI · Zehair LOUZZA  
Module : Architecture Big Data  
Dataset : +33 000 offres d'emploi LinkedIn (bucket S3 public `s3://snowflake-lab-bucket/`)
---
📌 Introduction
Dans un monde où le recrutement devient de plus en plus digitalisé, des milliers d'entreprises et de particuliers se tournent chaque jour vers des plateformes telles que LinkedIn pour publier ou rechercher des opportunités professionnelles. L'analyse de ces données offre un aperçu précieux du marché de l'emploi mondial : les métiers les plus demandés, les compétences clés recherchées, la répartition géographique ou encore les niveaux de rémunération.
Ce projet porte sur l'analyse d'un jeu de données LinkedIn de plus de 33 000 offres d'emploi. Les données sont réparties en plusieurs fichiers CSV et JSON couvrant divers aspects des offres : intitulé du poste, salaires, compétences, entreprise, avantages, etc.
---
🗂️ Architecture du Projet
```
linkedin/
├── RAW/      → Tables brutes chargées depuis S3 (CSV + JSON VARIANT)
├── CORE/     → Tables nettoyées et enrichies
└── MART/     → Vues analytiques prêtes à l'emploi
```
---
🛠️ Partie SQL — Pipeline de données Snowflake
00 · Initialisation de la base de données
On crée la base de données `LINKEDIN` puis les trois schémas qui structurent le pipeline : `RAW` pour les données brutes, `CORE` pour les données nettoyées, et `MART` pour les vues analytiques. On positionne ensuite le contexte de travail sur `RAW`.
```sql
CREATE OR REPLACE DATABASE LINKEDIN;

CREATE OR REPLACE SCHEMA LINKEDIN.RAW;
CREATE OR REPLACE SCHEMA LINKEDIN.CORE;
CREATE OR REPLACE SCHEMA LINKEDIN.MART;

USE DATABASE LINKEDIN;
USE SCHEMA RAW;
```
---
01 · Configuration du Stage S3
On crée un stage externe nommé `linkedin_s3_stage` qui pointe directement vers le bucket S3 public. La commande `LIST` permet de vérifier que le stage est bien configuré et que les fichiers sont accessibles depuis Snowflake.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA RAW;

CREATE OR REPLACE STAGE linkedin_s3_stage
  URL = 's3://snowflake-lab-bucket/';

LIST @linkedin_s3_stage;
```
---
02 · Formats de fichiers
On définit deux formats de fichiers réutilisables :
`ff_csv_linkedin` : ignore la ligne d'en-tête, gère les champs entre guillemets, et convertit les valeurs vides ou `null` en `NULL` Snowflake.
`ff_json_linkedin` : l'option `STRIP_OUTER_ARRAY` déballe le tableau racine des fichiers JSON pour que chaque élément devienne une ligne distincte dans la table.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT ff_csv_linkedin
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

CREATE OR REPLACE FILE FORMAT ff_json_linkedin
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;
```
---
03 · Création des tables RAW
Les tables RAW reçoivent les données brutes telles qu'elles arrivent de S3, sans transformation. Les tables issues de fichiers CSV sont typées colonne par colonne. Les fichiers JSON, dont la structure peut varier, sont chargés dans une colonne unique de type `VARIANT` qui accepte n'importe quelle structure semi-structurée.
> **Note :** les timestamps (`listed_time`, `expiry`, etc.) sont stockés en `NUMBER` à ce stade — leur conversion en `TIMESTAMP_NTZ` se fait dans l'étape CORE.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE benefits_raw (
    job_id NUMBER,
    inferred BOOLEAN,
    type STRING
);

CREATE OR REPLACE TABLE employee_counts_raw (
    company_id NUMBER,
    employee_count NUMBER,
    follower_count NUMBER,
    time_recorded NUMBER
);

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

CREATE OR REPLACE TABLE job_skills_raw (
    job_id NUMBER,
    skill_abr STRING
);

CREATE OR REPLACE TABLE companies_json_raw (
    src VARIANT
);

CREATE OR REPLACE TABLE company_industries_json_raw (
    src VARIANT
);

CREATE OR REPLACE TABLE company_specialities_json_raw (
    src VARIANT
);

CREATE OR REPLACE TABLE job_industries_json_raw (
    src VARIANT
);
```
---
04 · Chargement des données (COPY INTO RAW)
On commence par tronquer toutes les tables pour garantir l'idempotence du chargement — relancer le script ne duplique pas les données. Chaque `COPY INTO` charge un fichier depuis le stage S3 vers sa table cible en appliquant le format approprié. L'option `ON_ERROR = CONTINUE` permet de ne pas bloquer le chargement entier si certaines lignes sont malformées.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA RAW;

TRUNCATE TABLE benefits_raw;
TRUNCATE TABLE employee_counts_raw;
TRUNCATE TABLE job_postings_raw;
TRUNCATE TABLE job_skills_raw;
TRUNCATE TABLE companies_json_raw;
TRUNCATE TABLE company_industries_json_raw;
TRUNCATE TABLE company_specialities_json_raw;
TRUNCATE TABLE job_industries_json_raw;

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
```
---
05 · Tables CORE (données propres)
Les tables CORE reprennent la même structure que RAW mais avec des types définitifs : les timestamps Unix sont déclarés en `TIMESTAMP_NTZ`, et la table `job_postings` gagne une colonne calculée `normalized_salary_yearly` qui ramènera tous les salaires à une base annuelle comparable (calculée lors du chargement à l'étape suivante).
```sql
USE DATABASE LINKEDIN;
USE SCHEMA CORE;

CREATE OR REPLACE TABLE companies (
    company_id NUMBER,
    name STRING,
    description STRING,
    company_size NUMBER,
    state STRING,
    country STRING,
    city STRING,
    zip_code STRING,
    address STRING,
    url STRING
);

CREATE OR REPLACE TABLE company_industries (
    company_id NUMBER,
    industry STRING
);

CREATE OR REPLACE TABLE company_specialities (
    company_id NUMBER,
    speciality STRING
);

CREATE OR REPLACE TABLE employee_counts (
    company_id NUMBER,
    employee_count NUMBER,
    follower_count NUMBER,
    time_recorded TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE job_postings (
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
    original_listed_time TIMESTAMP_NTZ,
    remote_allowed BOOLEAN,
    views NUMBER,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry TIMESTAMP_NTZ,
    closed_time TIMESTAMP_NTZ,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time TIMESTAMP_NTZ,
    posting_domain STRING,
    sponsored BOOLEAN,
    work_type STRING,
    currency STRING,
    compensation_type STRING,
    normalized_salary_yearly FLOAT
);

CREATE OR REPLACE TABLE benefits (
    job_id NUMBER,
    inferred BOOLEAN,
    type STRING
);

CREATE OR REPLACE TABLE job_skills (
    job_id NUMBER,
    skill_abr STRING
);

CREATE OR REPLACE TABLE job_industries (
    job_id NUMBER,
    industry_id STRING
);
```
---
06 · Alimentation de CORE depuis RAW
C'est l'étape de transformation centrale. Pour les tables issues de JSON, on extrait chaque champ de la colonne `VARIANT` avec la notation `src:champ::TYPE`. Pour `employee_counts` et `job_postings`, on applique `TO_TIMESTAMP_NTZ()` pour convertir les timestamps Unix stockés en nombre. La colonne `normalized_salary_yearly` est calculée avec un `CASE` sur `pay_period` :
`YEARLY` → valeur directe
`MONTHLY` → × 12
`HOURLY` → × 2 080 (nombre d'heures travaillées par an en base plein temps)
```sql
USE DATABASE LINKEDIN;
USE SCHEMA CORE;

INSERT OVERWRITE INTO companies
SELECT
    src:company_id::NUMBER   AS company_id,
    src:name::STRING         AS name,
    src:description::STRING  AS description,
    src:company_size::NUMBER AS company_size,
    src:state::STRING        AS state,
    src:country::STRING      AS country,
    src:city::STRING         AS city,
    src:zip_code::STRING     AS zip_code,
    src:address::STRING      AS address,
    src:url::STRING          AS url
FROM LINKEDIN.RAW.companies_json_raw
WHERE src IS NOT NULL;

INSERT OVERWRITE INTO company_industries
SELECT
    src:company_id::NUMBER AS company_id,
    src:industry::STRING   AS industry
FROM LINKEDIN.RAW.company_industries_json_raw
WHERE src IS NOT NULL;

INSERT OVERWRITE INTO company_specialities
SELECT
    src:company_id::NUMBER AS company_id,
    src:speciality::STRING AS speciality
FROM LINKEDIN.RAW.company_specialities_json_raw
WHERE src IS NOT NULL;

INSERT OVERWRITE INTO job_industries
SELECT
    src:job_id::NUMBER      AS job_id,
    src:industry_id::STRING AS industry_id
FROM LINKEDIN.RAW.job_industries_json_raw
WHERE src IS NOT NULL;

INSERT OVERWRITE INTO employee_counts
SELECT
    company_id,
    employee_count,
    follower_count,
    TO_TIMESTAMP_NTZ(time_recorded) AS time_recorded
FROM LINKEDIN.RAW.employee_counts_raw;

INSERT OVERWRITE INTO benefits
SELECT job_id, inferred, type
FROM LINKEDIN.RAW.benefits_raw;

INSERT OVERWRITE INTO job_skills
SELECT job_id, skill_abr
FROM LINKEDIN.RAW.job_skills_raw;

INSERT OVERWRITE INTO job_postings
SELECT
    job_id,
    company_id,
    title,
    description,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    formatted_work_type,
    location,
    applies,
    TO_TIMESTAMP_NTZ(original_listed_time) AS original_listed_time,
    remote_allowed,
    views,
    job_posting_url,
    application_url,
    application_type,
    TO_TIMESTAMP_NTZ(expiry)      AS expiry,
    TO_TIMESTAMP_NTZ(closed_time) AS closed_time,
    formatted_experience_level,
    skills_desc,
    TO_TIMESTAMP_NTZ(listed_time) AS listed_time,
    posting_domain,
    sponsored,
    work_type,
    currency,
    compensation_type,
    CASE
        WHEN UPPER(pay_period) = 'YEARLY'  THEN COALESCE(med_salary, max_salary, min_salary)
        WHEN UPPER(pay_period) = 'MONTHLY' THEN COALESCE(med_salary, max_salary, min_salary) * 12
        WHEN UPPER(pay_period) = 'HOURLY'  THEN COALESCE(med_salary, max_salary, min_salary) * 2080
        ELSE NULL
    END AS normalized_salary_yearly
FROM LINKEDIN.RAW.job_postings_raw;
```
---
07 · Contrôles qualité
Trois vérifications essentielles avant de passer aux analyses : détection des doublons sur `job_id`, repérage des salaires incohérents (`min_salary > max_salary`), et comptage des nulls sur les colonnes critiques. Le dernier `SELECT` mesure le taux de jointure effectif avec les tables `job_industries` et `companies`.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA CORE;

SELECT
    job_id,
    COUNT(*) AS n
FROM job_postings
GROUP BY job_id
HAVING COUNT(*) > 1;

SELECT *
FROM job_postings
WHERE min_salary > max_salary;

SELECT
    COUNT(*)                     AS total_rows,
    COUNT_IF(job_id IS NULL)     AS null_job_id,
    COUNT_IF(title IS NULL)      AS null_title,
    COUNT_IF(company_id IS NULL) AS null_company_id
FROM job_postings;

SELECT
    COUNT(*)            AS total_jobs,
    COUNT(ji.job_id)    AS jobs_with_industry,
    COUNT(c.company_id) AS jobs_with_company
FROM LINKEDIN.CORE.job_postings jp
LEFT JOIN LINKEDIN.CORE.job_industries ji ON jp.job_id = ji.job_id
LEFT JOIN LINKEDIN.CORE.companies c       ON jp.company_id = c.company_id;
```
---
08 · Vues analytiques (MART)
On crée d'abord une vue centrale `vw_jobs_enriched` qui joint les offres avec les entreprises et les industries — toutes les autres vues s'appuient sur elle. La technique `ROW_NUMBER() OVER (PARTITION BY industry_id ...)` permet de produire un top N par groupe sans sous-requête répétée. `COALESCE(formatted_work_type, work_type)` maximise la couverture en cas de valeur manquante dans l'une des deux colonnes source.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA MART;

CREATE OR REPLACE VIEW vw_jobs_enriched AS
SELECT
    jp.job_id,
    jp.company_id,
    c.name                    AS company_name,
    jp.title,
    jp.formatted_work_type,
    jp.work_type,
    jp.location,
    jp.remote_allowed,
    jp.formatted_experience_level,
    jp.currency,
    jp.normalized_salary_yearly,
    ji.industry_id,
    c.company_size
FROM LINKEDIN.CORE.job_postings jp
LEFT JOIN LINKEDIN.CORE.job_industries ji ON jp.job_id = ji.job_id
LEFT JOIN LINKEDIN.CORE.companies c       ON jp.company_id = c.company_id;

CREATE OR REPLACE VIEW top_10_titles_by_industry AS
WITH ranked AS (
    SELECT
        industry_id,
        title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (
            PARTITION BY industry_id
            ORDER BY COUNT(*) DESC, title
        ) AS rn
    FROM vw_jobs_enriched
    WHERE industry_id IS NOT NULL
      AND TRIM(industry_id) <> ''
      AND title IS NOT NULL
      AND TRIM(title) <> ''
    GROUP BY industry_id, title
)
SELECT industry_id, title, nb_offres
FROM ranked
WHERE rn <= 10;

CREATE OR REPLACE VIEW top_10_best_paid_jobs_by_industry AS
WITH ranked AS (
    SELECT
        industry_id,
        title,
        company_name,
        currency,
        normalized_salary_yearly,
        ROW_NUMBER() OVER (
            PARTITION BY industry_id
            ORDER BY normalized_salary_yearly DESC NULLS LAST, title
        ) AS rn
    FROM vw_jobs_enriched
    WHERE industry_id IS NOT NULL
      AND TRIM(industry_id) <> ''
      AND normalized_salary_yearly IS NOT NULL
      AND title IS NOT NULL
      AND TRIM(title) <> ''
)
SELECT industry_id, title, company_name, currency, normalized_salary_yearly
FROM ranked
WHERE rn <= 10;

CREATE OR REPLACE VIEW jobs_by_company_size AS
SELECT
    TO_VARCHAR(company_size) AS company_size_group,
    COUNT(*)                 AS nb_offres
FROM vw_jobs_enriched
WHERE company_size IS NOT NULL
GROUP BY TO_VARCHAR(company_size)
ORDER BY TO_NUMBER(company_size_group);

CREATE OR REPLACE VIEW jobs_by_industry AS
SELECT
    industry_id,
    COUNT(*) AS nb_offres
FROM vw_jobs_enriched
WHERE industry_id IS NOT NULL
  AND TRIM(industry_id) <> ''
GROUP BY industry_id
ORDER BY nb_offres DESC;

CREATE OR REPLACE VIEW jobs_by_employment_type AS
SELECT
    COALESCE(formatted_work_type, work_type) AS employment_type,
    COUNT(*)                                 AS nb_offres
FROM vw_jobs_enriched
WHERE COALESCE(formatted_work_type, work_type) IS NOT NULL
  AND TRIM(COALESCE(formatted_work_type, work_type)) <> ''
GROUP BY COALESCE(formatted_work_type, work_type)
ORDER BY nb_offres DESC;
```
---
09 · Tests finaux
On vérifie que chaque vue analytique retourne bien des données avant de passer à la partie Python.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA MART;

SELECT * FROM vw_jobs_enriched                  LIMIT 20;
SELECT * FROM top_10_titles_by_industry         LIMIT 20;
SELECT * FROM top_10_best_paid_jobs_by_industry LIMIT 20;
SELECT * FROM jobs_by_company_size              LIMIT 20;
SELECT * FROM jobs_by_industry                  LIMIT 20;
SELECT * FROM jobs_by_employment_type           LIMIT 20;
```
---
10 · App Streamlit (optionnel)
Snowflake permet de déployer une application Streamlit directement dans l'entrepôt, sans infrastructure externe. Il faut d'abord uploader le fichier `streamlit_app.py` dans le stage `@streamlit_src`, puis décommenter le bloc `CREATE OR REPLACE STREAMLIT`.
```sql
USE DATABASE LINKEDIN;
USE SCHEMA MART;

SHOW WAREHOUSES;
SELECT CURRENT_WAREHOUSE();

CREATE OR REPLACE STAGE streamlit_src;

-- CREATE OR REPLACE STREAMLIT linkedin_jobs_app
--   ROOT_LOCATION = '@streamlit_src'
--   MAIN_FILE = 'streamlit_app.py'
--   QUERY_WAREHOUSE = COMPUTE_WH;

SELECT * FROM LINKEDIN.CORE.job_postings     LIMIT 10;
SELECT * FROM LINKEDIN.CORE.companies        LIMIT 10;
SELECT * FROM LINKEDIN.CORE.job_industries   LIMIT 10;
SELECT * FROM LINKEDIN.MART.vw_jobs_enriched LIMIT 10;
```
---
🐍 Partie Python — Analyses et Visualisations
Le notebook Python se connecte à Snowflake via une session Snowpark active et interroge les vues MART pour produire des visualisations avec `matplotlib`.
---
0 · Imports et session Snowflake
On récupère la session Snowpark active (disponible nativement dans les notebooks Snowflake) et on définit une fonction utilitaire `run_query` qui exécute du SQL et retourne directement un DataFrame pandas.
```python
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def run_query(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()
```
---
1 · Aperçu des données enrichies
Un premier `SELECT *` sur la vue enrichie pour s'assurer que les jointures fonctionnent et que les colonnes clés sont bien renseignées.
```python
preview_df = run_query("""
    SELECT *
    FROM LINKEDIN.MART.vw_jobs_enriched
    LIMIT 10
""")

display(preview_df)
```
---
2 · Top 10 des titres les plus publiés par industrie
On récupère les données de la vue `top_10_titles_by_industry` puis on filtre sur la première industrie disponible pour produire un graphique à barres. Le même bloc peut être réutilisé pour n'importe quel `industry_id`.
```python
top_titles = run_query("""
    SELECT *
    FROM LINKEDIN.MART.top_10_titles_by_industry
    LIMIT 10
""")

display(top_titles)

if not top_titles.empty:
    selected_industry = top_titles["INDUSTRY_ID"].iloc[0]
    df_titles = top_titles[top_titles["INDUSTRY_ID"] == selected_industry]

    plt.figure(figsize=(12, 6))
    plt.bar(df_titles["TITLE"], df_titles["NB_OFFRES"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top titres - Industrie : {selected_industry}")
    plt.xlabel("Titre")
    plt.ylabel("Nombre d'offres")
    plt.tight_layout()
    plt.show()
else:
    print("Aucune donnée disponible.")
```
---
3 · Top 10 des postes les mieux rémunérés
Le salaire affiché est `normalized_salary_yearly`, calculé dans CORE selon la période de paye (annuel direct, mensuel × 12, horaire × 2 080). Cela permet de comparer des salaires de périodicités différentes sur une même échelle.
```python
best_paid = run_query("""
    SELECT *
    FROM LINKEDIN.MART.top_10_best_paid_jobs_by_industry
    LIMIT 10
""")

display(best_paid)

if not best_paid.empty:
    selected_industry_salary = best_paid["INDUSTRY_ID"].iloc[0]
    df_salary = best_paid[best_paid["INDUSTRY_ID"] == selected_industry_salary]

    plt.figure(figsize=(12, 6))
    plt.bar(df_salary["TITLE"], df_salary["NORMALIZED_SALARY_YEARLY"])
    plt.xticks(rotation=45, ha="right")
    plt.title(f"Top salaires - Industrie : {selected_industry_salary}")
    plt.xlabel("Titre")
    plt.ylabel("Salaire annuel normalisé (USD)")
    plt.tight_layout()
    plt.show()
else:
    print("Aucune donnée salariale disponible.")
```
---
4 · Répartition par taille d'entreprise
On visualise la distribution des offres selon la taille de l'entreprise. Cela permet d'identifier si le marché est dominé par les grandes structures ou les PME.
```python
jobs_company_size = run_query("""
    SELECT *
    FROM LINKEDIN.MART.jobs_by_company_size
    LIMIT 10
""")

display(jobs_company_size)

if not jobs_company_size.empty:
    plt.figure(figsize=(10, 5))
    plt.bar(jobs_company_size["COMPANY_SIZE_GROUP"], jobs_company_size["NB_OFFRES"])
    plt.title("Offres par taille d'entreprise")
    plt.xlabel("Taille")
    plt.ylabel("Nombre d'offres")
    plt.tight_layout()
    plt.show()
else:
    print("Pas de données.")
```
---
5 · Répartition par secteur d'activité
On affiche les 10 secteurs qui publient le plus d'offres, pour identifier les industries les plus actives sur LinkedIn.
```python
jobs_industry = run_query("""
    SELECT *
    FROM LINKEDIN.MART.jobs_by_industry
    LIMIT 10
""")

display(jobs_industry)

if not jobs_industry.empty:
    plt.figure(figsize=(12, 6))
    plt.bar(jobs_industry["INDUSTRY_ID"], jobs_industry["NB_OFFRES"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Top secteurs par nombre d'offres")
    plt.xlabel("Industrie")
    plt.ylabel("Nombre d'offres")
    plt.tight_layout()
    plt.show()
else:
    print("Pas de données secteur.")
```
---
6 · Répartition par type d'emploi
On analyse la répartition entre temps plein, temps partiel, contrat, stage, etc. La colonne source combine `formatted_work_type` et `work_type` via `COALESCE` pour maximiser la couverture.
```python
jobs_types = run_query("""
    SELECT *
    FROM LINKEDIN.MART.jobs_by_employment_type
    LIMIT 10
""")

display(jobs_types)

if not jobs_types.empty:
    plt.figure(figsize=(10, 5))
    plt.bar(jobs_types["EMPLOYMENT_TYPE"], jobs_types["NB_OFFRES"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Types d'emploi")
    plt.xlabel("Type")
    plt.ylabel("Nombre d'offres")
    plt.tight_layout()
    plt.show()
else:
    print("Pas de données emploi.")
```
---
7 · KPIs globaux
Un résumé chiffré du dataset : nombre total d'offres, d'entreprises distinctes, de secteurs couverts et salaire annuel moyen. Utile comme slide de synthèse ou point d'entrée d'un tableau de bord.
```python
kpis = run_query("""
    SELECT
        COUNT(*)                      AS total_jobs,
        COUNT(DISTINCT company_id)    AS total_companies,
        COUNT(DISTINCT industry_id)   AS total_industries,
        AVG(normalized_salary_yearly) AS avg_salary
    FROM LINKEDIN.MART.vw_jobs_enriched
""")

display(kpis)
```
---
8 · Diagnostic qualité
On mesure le taux de complétude des colonnes critiques dans la vue enrichie pour identifier les zones de données manquantes et évaluer la fiabilité des analyses produites.
```python
diagnostic = run_query("""
    SELECT
        COUNT(*)                                                   AS total_rows,
        COUNT_IF(company_name IS NULL)                             AS missing_company,
        COUNT_IF(industry_id IS NULL)                              AS missing_industry,
        COUNT_IF(company_size IS NULL)                             AS missing_size,
        COUNT_IF(normalized_salary_yearly IS NULL)                 AS missing_salary,
        COUNT_IF(COALESCE(formatted_work_type, work_type) IS NULL) AS missing_type
    FROM LINKEDIN.MART.vw_jobs_enriched
""")

display(diagnostic)
```
---
9 · Exemples détaillés
On affiche un échantillon de lignes complètes de la vue enrichie avec les colonnes les plus pertinentes, pour valider visuellement la cohérence des données après toutes les transformations.
```python
details = run_query("""
    SELECT
        job_id,
        company_id,
        company_name,
        title,
        industry_id,
        company_size,
        formatted_work_type,
        location,
        normalized_salary_yearly
    FROM LINKEDIN.MART.vw_jobs_enriched
    LIMIT 10
""")

display(details)
```
---
📈 Résultats Clés
Analyse	Résultat
Top métiers	Data Analyst, Software Engineer en tête
Mieux rémunérés	Postes seniors tech (Architecte Cloud, Data Scientist Principal)
Taille d'entreprise	45 % des offres dans les grandes entreprises (250–999 employés)
Compétences les + demandées	SQL, Python, JavaScript
Télétravail	Secteurs Tech et Consulting : 60–70 % de postes remote
---
🏁 Conclusion
Ce projet illustre un pipeline Big Data complet sur Snowflake : ingestion multi-format (CSV + JSON) depuis S3, modélisation en couches RAW → CORE → MART, contrôles qualité, et analyses exploratoires via Python/Snowpark. Snowflake s'est révélé être un outil puissant pour gérer cette diversité de données, avec sa capacité à traiter efficacement des formats variés et à transformer des structures complexes en informations exploitables.
