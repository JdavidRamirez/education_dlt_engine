# Education_dlt_engine

This project involves the implementation of a Medallion Architecture data pipeline using Databricks, Delta Live Tables (DLT) and PySpark. 

It ingests raw data from multiple Google Sheets, such as those containing information on schools, teachers, students, and attendance. The project consists of a job that acts as an orchestrator to process a pipeline that drives the data through the Bronze, Silver, and Gold layers and creates a star schema.

## Architecture

The pipeline follows the Lakehouse architecture pattern:

* **Bronze Layer**: Ingests raw data directly from Google Sheets into Delta tables without modification.
* **Silver Layer**: Standardizes data types and removes duplicates to ensure data quality and integrity.
* **Gold Layer**: Integrates the processed data into a unified Star Schema, optimized for analytics and reporting.

   <img width="420" height="348" alt="image" src="https://github.com/user-attachments/assets/9c9368d6-50f1-49ec-8904-d83c6c8eeb36" />


##  Tech Stack

* **Platform:** Databricks
* **Language:** Python (PySpark)
* **Framework:** Delta Live Tables (DLT)
* **Orchestration:** Databricks Workflows
* **Storage:** Delta Lake

##  Pipeline Logic

### 1. Ingestion (01_fetch_data.py)

* Retrieves five datasets directly from Google Sheets.
* Adds an ingestion timestamp to support data versioning and history.
* Appends new records to the landing schema, automatically handling any structure changes.

### 2. Transformation (02_dlt_pipeline.py)

* Data Quality: Enforces validation rules (via @dlt.expect) to ensure valid emails, positive class durations, and correct attendance statuses.
* Deduplication: Updates records and removes duplicates using dlt.apply_changes() based on the most recent timestamp.
* Gold Layer: Joins the cleaned tables into a unified Fact Table (gold_fact_attendance) to facilitate analysis by school, teacher, or grade.

##  Results

<img width="392" height="335" alt="image" src="https://github.com/user-attachments/assets/c0256ddc-8ded-4587-9b56-729cf1ef0d44" />


##  How to Run
1.  Clone this repository.
2.  Upload the files in `notebooks/` to a Databricks Workspace.
3.  Run `00_setup_architecture` to create the project architecture.
4.  Create a **Job** with two tasks:
    * **Task 1:** Run `01_fetch_data`.
    * **Task 2:** Trigger the DLT Pipeline `02_dlt_pipeline`.
