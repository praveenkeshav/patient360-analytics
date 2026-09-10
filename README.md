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
