This pipeline provides:

Reliable CDC handling

Clear separation of concerns

Scalable, cost-efficient execution

A strong foundation for analytics and ML workloads

# Databricks Medallion Architecture

## Project Summary
This project implements the Medallion Architecture (Bronze, Silver, Gold) on Databricks Community Edition (CE), focusing on secure storage, robust orchestration, and reliable data quality.

## 1. Environment Setup
- **Platform:** Databricks Community Edition (CE)
- **Storage Standard:** Transitioned from legacy DBFS mounts (`/mnt/`) to Workspace Files and Hive Metastore due to CE security restrictions.
- **Orchestration:** Managed via Databricks Workflows, configured to run notebooks directly from the Workspace (bypassing Repo-internal path restrictions).

## 2. Pipeline Architecture (Medallion)

### Bronze Layer (`default.events_raw`)
- Ingests raw source files (JSON/CSV) from Workspace paths.
- Performs initial data validation to ensure the schema is correctly interpreted by the Hive Metastore.

### Silver Layer (`silver.events`)
- **Schema Creation:** Explicitly creates the silver schema in the Metastore.
- **Transformations:** Handles timestamp parsing (`to_timestamp`), null-filtering on `event_id`, and deduplication.
- **Data Quality:** Includes assert statements to verify zero nulls and zero duplicates before proceeding to Gold.

### Gold Layer (`gold.daily_metrics`)
- **Aggregation:** Groups data by `event_date` and `event_type`.
- **Schema Evolution:** Uses `.option("overwriteSchema", "true")` to ensure the final business-level metrics remain consistent.

## 3. Critical Troubleshooting & Fixes
- **Git Integration:** Resolved "Internal Commit" errors by migrating notebooks from `/Repos/` to `/Users/[Email]/`. Jobs in CE require Workspace as the source, not Git.
- **Path Resolution:** Fixed `UnsupportedOperationException` by avoiding the disabled `/mnt/` root and utilizing absolute Workspace paths.
- **Namespace Correction:** Adapted code to the Hive Metastore single-tier namespace after identifying that the `main` (Unity Catalog) was unavailable in this workspace tier.

