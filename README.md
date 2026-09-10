# Patient 360 — Healthcare Analytics Engineering

## 1. Project Overview

Patient 360 is an independent healthcare analytics engineering project that demonstrates how heterogeneous synthetic EHR and claims data can be transformed into trusted, analytics-ready datasets and business intelligence.

![Patient 360 End-to-End Analytics Workflow](docs/images/patient360-end-to-end-workflow.png)

The project focuses on the complete engineering lifecycle:

- Source profiling and data assessment
- Python/Pandas preprocessing and healthcare data validation
- Automated Python testing with pytest
- AWS S3-based data handoff
- Apache Airflow workflow orchestration
- Astronomer Cosmos integration for dbt orchestration
- Snowflake RAW data ingestion and auditability
- dbt-based transformation and dimensional modeling
- Automated data-quality testing
- Git-based CI/CD and production deployment
- Power BI reporting, refresh automation, and KPI reconciliation

### Business Objective

The objective is to provide a unified analytical view across:

- Patients
- Encounters
- Diagnoses and chronic conditions
- Laboratory observations
- Claims
- Claim transactions

The resulting analytical layer supports healthcare use cases including:

- Patient population analysis
- Encounter and hospital utilization analysis
- 30-day readmissions
- Length of stay (LOS)
- Laboratory abnormality analysis
- Chronic disease prevalence
- Claims and healthcare cost analysis

### End-to-End Flow

```text
Synthea EHR / Claims / FHIR Data
              |
              v
      Python + pandas
   Preprocessing / Validation
              |
              v
            pytest
              |
              v
        AWS S3 Landing
              |
              v
        AWS S3 Processed
              |
              v
       Apache Airflow
              |
              v
       Snowflake RAW
              |
              v
   Astronomer Cosmos + dbt
              |
              v
       dbt STAGING
              |
              v
     dbt INTERMEDIATE
              |
              v
       dbt MARTS
              |
              v
          Power BI
```

The project uses a production-style engineering workflow for demonstration and learning. It is not a production healthcare system and does not use real patient data.

---

## 2. Architecture and Technology Stack

### Architecture

```text
                         SOURCE DATA
                 Synthea CSV / FHIR / JSON
                              |
                              v
                 +-------------------------+
                 | Python + pandas         |
                 | Preprocessing           |
                 | Data preparation        |
                 | Healthcare validation   |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | pytest                  |
                 | Automated validation    |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | AWS S3                  |
                 | Landing / Processed     |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | Apache Airflow          |
                 | Workflow orchestration  |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | Snowflake               |
                 | PATIENT360_PROD         |
                 | RAW                     |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | Astronomer Cosmos       |
                 | dbt orchestration       |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | dbt                     |
                 | STAGING                 |
                 | INTERMEDIATE            |
                 | MARTS                   |
                 +------------+------------+
                              |
                              v
                 +-------------------------+
                 | Power BI                |
                 | Patient 360 Analytics   |
                 +-------------------------+

       Git-based CI/CD
       Git -> GitHub -> Pull Request -> CI -> Review
                                      -> main
                                      -> Production Deployment
```

### Technology Stack

| Area | Technology |
|---|---|
| Source data | Synthea, CSV, FHIR/JSON |
| Programming | Python |
| Data processing | pandas |
| Python testing | pytest |
| Cloud storage | AWS S3 |
| Workflow orchestration | Apache Airflow |
| Airflow runtime | Astronomer Astro Runtime |
| dbt orchestration | Astronomer Cosmos |
| Containerization | Docker |
| Data warehouse | Snowflake |
| Transformation | dbt + SQL |
| Source control | Git / GitHub |
| CI/CD | GitHub Actions |
| Business intelligence | Power BI |
| BI authentication | Microsoft Entra service principal |
| BI automation | Power BI REST API |

---

## 3. Source Data and Python Engineering

### Source Data

The project uses synthetic healthcare data generated with Synthea.

The source datasets include:

- Patients
- Encounters
- Conditions / diagnoses
- Laboratory observations
- Claims
- Claim transactions
- FHIR resources

Both CSV-based datasets and FHIR/JSON clinical resources are used.

### Source Profiling

Before implementing the pipeline, the source data was profiled to understand:

- Dataset structure and column definitions
- Data types
- Business identifiers
- Entity relationships
- Date fields
- Nullability
- Duplicate records
- Referential integrity
- Source-data limitations
- Analytical grain

This profiling informed the ingestion, validation, and downstream data-modeling decisions.

### Python / Pandas Objective

Python is used as the source-data engineering and validation layer.

The primary objective is to prepare heterogeneous healthcare source data for reliable downstream ingestion while applying validation rules before data reaches the warehouse.

The Python implementation includes:

- Data-type normalization
- Date parsing and standardization
- Column-name standardization
- Source-data preparation
- Derived analytical fields
- Referential validation
- Clinical data validation
- Laboratory data processing
- FHIR/JSON resource processing

Processing logic is separated from validation logic so that validation functions can be independently tested.

### FHIR / Clinical Data Processing

FHIR/JSON resources are converted into analytical structures where required.

For laboratory observations, the processing flow is:

```text
FHIR / JSON
    |
    v
Resource Parsing
    |
    v
Observation Extraction
    |
    v
Reference Range Processing
    |
    v
Abnormality Classification
    |
    v
Processed Analytical Data
```

### Python Automated Testing

pytest is used to test the implemented Python validation components.

Validation includes rules for:

- Required identifiers
- Valid dates
- Patient references
- Encounter references
- Identifier relationships
- Clinical data
- Laboratory data

---

## 4. Data Storage, Ingestion and Airflow Orchestration

### AWS S3

AWS S3 provides the cloud storage boundary between source processing and warehouse ingestion.

```text
Source
  |
  v
S3 Landing
  |
  v
Python Processing / Validation
  |
  v
S3 Processed
  |
  v
Snowflake RAW
```

The separation between landing and processed data provides a controlled handoff between source-data processing and warehouse ingestion.

### Apache Airflow

Apache Airflow is the workflow orchestration layer.

The implementation uses focused DAGs for different pipeline responsibilities, including:

- EHR ingestion
- Claims ingestion
- Snowflake RAW loading
- dbt transformation
- Scheduled dbt refresh
- Power BI refresh

Airflow provides:

- Task dependencies
- Scheduling
- Retries
- Timeouts
- Failure propagation
- Sensor-based workflow control
- Operational visibility

Python is responsible for processing and validation; Airflow coordinates execution and workflow dependencies.

### Astronomer Cosmos + dbt Orchestration

Astronomer Cosmos is explicitly implemented to integrate the dbt project with Apache Airflow.

The dbt project is represented inside Airflow using Cosmos `DbtTaskGroup`.

Conceptually:

```text
Airflow DAG
     |
     v
Astronomer Cosmos
     |
     v
DbtTaskGroup
     |
     v
dbt Project Graph
     |
     +---- STAGING models
     |
     +---- INTERMEDIATE models
     |
     +---- MART models
     |
     +---- dbt tests
```

This allows the dbt dependency graph to participate directly in the Airflow workflow instead of treating dbt as an unrelated external process.

Cosmos handles dbt project parsing and maps dbt resources into Airflow tasks, providing Airflow-level dependency management, execution, retries, and operational monitoring.

The production dbt DAG uses Cosmos configuration for the project, Snowflake profile, rendering, and dbt execution.

---

## 5. Snowflake and dbt Transformation

### Snowflake

The analytical warehouse environment is:

```text
PATIENT360_PROD
```

The data architecture separates source-oriented data from analytical transformations:

```text
RAW
 |
 v
STAGING
 |
 v
INTERMEDIATE
 |
 v
DIMENSIONAL MARTS
 |
 v
Power BI
```

### Snowflake RAW Layer

The RAW layer contains source-oriented healthcare data such as:

- Patients
- Encounters
- Diagnoses / conditions
- Laboratory observations
- Claims
- Claim transactions

The RAW layer is intentionally kept separate from analytical transformations to support:

- Source traceability
- Controlled ingestion
- Troubleshooting
- Reproducibility
- A stable foundation for dbt

### RAW Load Audit

The RAW ingestion process includes audit information associated with warehouse loads, including:

- Target table
- Source file
- Rows loaded
- Load status
- DAG ID
- DAG run ID
- Task ID

This provides traceability between source data, Airflow execution, and Snowflake RAW objects.

### dbt Transformation

dbt performs SQL-based analytical transformation inside Snowflake.

```text
Snowflake RAW
     |
     v
dbt STAGING
     |
     v
dbt INTERMEDIATE
     |
     v
dbt MARTS
```

#### STAGING

Staging models standardize source structures and provide consistent interfaces for downstream transformations.

Examples include:

```text
stg_patients
stg_encounters
stg_diagnoses
stg_labs
```

#### INTERMEDIATE

Intermediate models contain reusable transformation and business logic between standardized source models and final analytical models.

#### MARTS

The MART layer implements a dimensional model for analytical consumption.

Core analytical domains include:

- Patient
- Encounter
- Diagnosis / chronic disease
- Laboratory
- Claims
- Claim transactions
- Date / time dimensions where applicable

The model separates business processes into fact tables and descriptive entities into dimensions.

Incremental dbt models are used where appropriate for production-style transformation workflows.

### Claims Grain Design

A key modeling issue was identifying that claims data contained repeated business keys and represented more than one analytical grain.

The implementation distinguishes:

```text
Claim-level grain
        |
        +---- stg_claims
        |       One row per claim
        |
        +---- stg_claim_transactions
                One row per transaction
```

The downstream claim fact remains aligned to claim-level grain.

This prevents transaction-level duplication from incorrectly inflating claim-level analytics.

The issue was resolved through source-grain profiling and model redesign rather than masking the problem with an arbitrary surrogate-key workaround.

---

## 6. Data Quality, Testing and Validation

Data quality is treated as a cross-layer concern rather than a single final check.

```text
Source Profiling
      |
      v
Python Validation
      |
      v
pytest
      |
      v
S3 Processed Data
      |
      v
Snowflake RAW Validation
      |
      v
RAW Load Audit
      |
      v
dbt Tests
      |
      v
Analytical Models
      |
      v
Production Validation
      |
      v
Power BI Validation
```

### Validation Controls

The implemented validation framework includes controls such as:

- Identifier validity
- Null checks
- Uniqueness
- Referential integrity
- Date validation
- Row-count validation
- Freshness validation
- dbt model tests
- Business-rule validation
- KPI reconciliation
- Anomaly and data-quality checks

### Production Validation

After production deployment, validation covers both technical and analytical correctness.

```text
Production Deployment
        |
        v
Snowflake Objects / Schema
        |
        v
Row Counts / Freshness
        |
        v
dbt Tests
        |
        v
Business Rules
        |
        v
KPI Reconciliation
        |
        v
Power BI Validation
```

The objective is to verify:

1. The deployment completed successfully.
2. The resulting analytical data is correct and usable.

---

## 7. Git-Based CI/CD and Production Deployment

Git is used as the source-control foundation for development and deployment.

### Development Workflow

```text
Local Development
       |
       v
Feature Branch
       |
       v
Implementation
       |
       v
Automated Tests
       |
       v
Pull Request
       |
       v
GitHub Actions CI
       |
       v
Code Review
       |
       v
Merge to main
```

### Continuous Integration

GitHub Actions provides the automated CI quality gate.

CI validates areas including:

- Project configuration
- dbt project structure
- dbt parsing
- Dependencies
- Transformation logic
- Automated tests

The purpose of CI is to catch issues before changes are promoted to production.

### Production Deployment

After successful review and merge, the production workflow continues through GitHub Actions:

```text
Merge to main
     |
     v
Production Deployment
     |
     v
Production Configuration
     |
     v
dbt Dependencies
     |
     v
dbt Parse
     |
     v
dbt Build
     |
     v
Production Materialization
     |
     v
Post-Deployment Validation
```

The production environment targets:

```text
PATIENT360_PROD
```

Production configuration and secrets are maintained through the GitHub production Environment.

This establishes a version-controlled promotion path from development to production rather than making production changes manually.

---

## 8. Power BI and Healthcare Analytics

### Patient 360 Reporting

Power BI provides the business intelligence layer over the curated analytical models.

The Patient 360 Overview supports the defined healthcare KPI scope and provides filters such as:

- Age group
- Gender
- Race
- Ethnicity
- Encounter class

Supporting reporting includes:

- Chronic disease prevalence
- Encounter volume by class
- Patient 360 KPI cards

Power BI consumes curated analytical data rather than raw source datasets.

### KPI Scope

The final analytical KPI scope includes:

1. 30-Day Readmission Rate
2. Average Length of Stay (LOS)
3. Lab Abnormality Rate
4. Chronic Disease Prevalence
5. Total Claim Cost
6. Average Claim Cost
7. Claims per Patient
8. Claim Transaction Count

### Power BI Refresh Automation

Power BI refresh is automated through:

- Microsoft Entra service principal
- OAuth 2.0 client credentials
- Power BI REST API
- Airflow orchestration

Conceptually:

```text
Airflow
   |
   v
Microsoft Entra Authentication
   |
   v
Power BI REST API
   |
   v
Workspace / Semantic Model
   |
   v
Refresh
```

Airflow acts as the orchestration point for the downstream refresh workflow.

Power BI's native scheduled refresh is disabled where the API-based workflow is used, avoiding two independent scheduling mechanisms for the same refresh process.

### KPI Reconciliation

The reporting layer is validated against the analytical warehouse:

```text
Snowflake MART
      |
      v
Independent KPI Calculation
      |
      v
Power BI KPI
      |
      v
Reconciliation
```

This validates that dashboard KPIs represent the underlying analytical data correctly.

Power BI validation includes semantic-model availability, refresh status, dashboard rendering, KPI values, filter behavior, data reconciliation, and API refresh verification.

---

## 9. Security and Repository Structure

### Security and Secrets

The implementation keeps sensitive configuration outside source control.

Controls include:

- Credentials excluded from source code
- `.env` excluded through `.gitignore`
- Snowflake key files protected from source control
- Private key material kept outside the repository
- Production credentials stored in the GitHub production Environment
- Runtime secret injection
- Power BI authentication maintained through Airflow connection configuration
- Secrets excluded from project documentation

No passwords, private keys, client secrets, or environment-specific secret values are documented in this README.

### Repository Structure

```text
patient360-analytics/
|
+-- .github/
|   +-- workflows/
|
+-- dags/
|   +-- claims_ingestion_dag.py
|   +-- ehr_ingestion_dag.py
|   +-- ehr_raw_load_dag.py
|   +-- dbt_models_dag.py
|   +-- dbt_weekly_refresh_dag.py
|   +-- patient360_powerbi_refresh_dag.py
|
+-- dbt/
|   +-- Patient 360 dbt project
|
+-- src/
|   +-- preprocessing/
|   +-- validation/
|
+-- tests/
|
+-- sql/
|
+-- scripts/
|
+-- data/
|
+-- Dockerfile
+-- .gitignore
+-- requirements.txt
+-- packages.txt
+-- README.md
```

---

## 10. Scope, Limitations and Completion Boundary

### Project Scope

The technical implementation covers:

- Source profiling
- Python/Pandas preprocessing
- FHIR/JSON clinical processing
- Data validation
- pytest testing
- S3 data handoff
- Apache Airflow orchestration
- Astronomer Cosmos dbt orchestration
- Snowflake RAW ingestion
- RAW load auditing
- dbt transformation
- Dimensional modeling
- dbt testing
- Git/GitHub development workflow
- GitHub Actions CI/CD
- Production deployment
- Production data validation
- Power BI reporting
- KPI reconciliation
- Power BI REST API refresh automation
- Security and secrets management

### Known Limitations

The project uses synthetic Synthea data.

Therefore:

- Data distributions should not be interpreted as real-world population statistics.
- Clinical values are synthetic.
- Claims and financial measures are synthetic.
- Results are intended for engineering, analytics, and BI demonstration.
- Source limitations are documented rather than artificially corrected solely to create realistic-looking populations.

### Completion Boundary

The project demonstrates a completed technical engineering workflow through production-style deployment and validation.

Formal stakeholder UAT and business production sign-off are not represented as completed engineering activities in this repository.

### End-to-End Implementation Summary

```text
Synthea Healthcare Data
        |
        v
Source Profiling
        |
        v
Python / pandas
        |
        v
Validation + pytest
        |
        v
AWS S3
        |
        v
Apache Airflow
        |
        v
Snowflake RAW + Load Audit
        |
        v
Astronomer Cosmos
        |
        v
dbt STAGING
        |
        v
dbt INTERMEDIATE
        |
        v
dbt MARTS
        |
        v
dbt Tests
        |
        v
Power BI
        |
        v
KPI Reconciliation / Dashboard Validation


Git-Based Delivery Path

Feature Branch
        |
        v
Pull Request
        |
        v
GitHub Actions CI
        |
        v
Code Review
        |
        v
Merge to main
        |
        v
Production Deployment
        |
        v
dbt Production Build
        |
        v
Post-Deployment Validation
```

### Technology Summary

**Python + pandas + pytest**  
Source preprocessing, healthcare data preparation, validation, and automated testing.

**AWS S3**  
Landing and processed-data storage.

**Apache Airflow + Astronomer Cosmos**  
Workflow orchestration and direct dbt integration through Cosmos.

**Snowflake**  
RAW data storage, analytical warehouse, and production data platform.

**dbt + SQL**  
Layered transformation, incremental models, dimensional modeling, and data-quality testing.

**Git + GitHub + GitHub Actions**  
Version control, pull-request workflow, CI quality gates, and production deployment.

**Power BI + Microsoft Entra + Power BI REST API**  
Healthcare analytics, reporting, authenticated refresh automation, and dashboard validation.

---

## Synthetic Data Disclaimer

This project uses synthetic healthcare data generated using Synthea. It does not contain real patient information and is intended for demonstrating healthcare data engineering, analytics engineering, data quality, orchestration, CI/CD, and BI practices.

Official Synthea source/download page:

https://synthea.mitre.org/downloads
