# Table of Content

- [DataHub DBT Project](#datahub-dbt-project)
  - [DataHub DBT Project Overview](#datahub-dbt-project-overview)
    - [Folder Structure](#folder-structure)
      - [Per-Source Layers](#per-source-layers)
        - [staging/](#staging)
        - [int/ (intermediate)](#int-intermediate)
        - [core/](#core)
      - [Public Business Layer](#public-business-layer)
    - [Naming Conventions](#naming-conventions)
    - [Key Macros](#key-macros)
- [Schema & Database Setup with dbt Macro](#schema--database-setup-with-dbt-macro)
  - [Usage](#usage-schema)
    - [Define your schema structure](#1-define-your-schema-structure)
    - [Create the required Snowflake tag](#2-create-the-required-snowflake-tag-once-per-database)
    - [Run the macro for the desired environment](#3-run-the-macro-for-the-desired-environment)
- [Storage Integration & Stages](#storage-integration--stages)
  - [Storage Integration](#storage-integration)
  - [Storage Integration IAM](#storage-integration-iam)
  - [External Stages](#external-stages)
  - [External Tables](#external-tables)
- [DBT](#dbt)

---

# DataHub DBT Project
This project is designed to manage data transformations and orchestrations using dbt in a Snowflake environment. It includes macros for schema creation, storage integration, and external stages, specifically tailored for Azure Blob Storage.

## DataHub DBT Project Overview

This dbt project follows a layered architecture inspired by the Medallion pattern, with a standardized folder and macro structure to process data from source to curated outputs.

### Folder Structure

Each data source is organized into its own directory under `models/`, for example:

```bash
models/
├── <source_1>/
│  ├── staging/
│  ├── int/
│  └── core/
├── public/
│  ├── <product_1>/
```

#### Per-Source Layers

Each source folder (e.g. `open_meteo`) contains three subfolders:

- **`staging/`**
  Loads raw external tables and enriches them with metadata and quality flags.
  Based on the `staging_layer` macro, it:
  - Refreshes external data sources using pre-hooks
  - Adds technical metadata (filename, row number, timestamps)
  - Applies optional row-level quality checks
  - Supports incremental loads via file modification timestamps
  ➤ **Intended for internal use by the Data Engineering team only**

- **`int/` (intermediate)**
  Transforms validated staging data into deduplicated, hash-tracked records.
  Using the `int_layer` macro, it:
  - Applies optional type casting
  - Filters out invalid rows
  - Calculates `business_hash_key` and `hash_diff`
  - Supports incremental insert-only loads by comparing hashes
  ➤ **Intended for internal use by the Data Engineering team only**

- **`core/`**
  Optionally used for further normalization, renaming, historization, and data modeling.
  Core models serve as clean, consistent base tables for analytics and downstream use.
  ➤ **Shared with Data Analysts and downstream consumers**

#### Public Business Layer

- **`public/`**
  Contains business-facing marts and KPI models.
  Organized by product or domain, with subfolders like `product_name/`.
  Models here combine data from multiple sources and are tailored for reporting or dashboards.
  ➤ **Visible and consumable by business users and analysts**

### Naming Conventions

- `stg_<source>__<table>` – Staging Layer
- `int_<source>__<entity>` – Intermediate Layer
- `core_<source>__<entity>` – Core Layer (shared)
- `mart_<product>__<topic>` – Business Marts (public layer)

### Key Macros

- **`staging_layer(...)`**
  Loads external tables, enriches with metadata and applies quality checks.

- **`int_layer(...)`**
  Filters, casts, deduplicates and tracks changes with hash keys for incremental loading.

---

# Schema & Database Setup with dbt Macro

This project includes a dbt macro to create databases and schemas in Snowflake based on a defined structure (`db_schema_structure` variable). It supports both development (`dev`) and production (`prod`) environments.

## Prerequisites

- The dbt profile must define `dev` and `prod` targets.
- Required Snowflake privileges:
  - `CREATE DATABASE`
  - `CREATE SCHEMA`
  - `CREATE TAG`
  - `APPLY TAG orchestrated_by`
- The tag `orchestrated_by` must exist in your Snowflake account.

## Usage (schema)

### 1. Define your schema structure

In `dbt_project.yml` under `vars`:

```yaml
vars:
  db_schema_structure:
    DH_RAW:
      - SOURCE
      - OPEN_METEO
      - <ADD MORE SOURCES>
    DH_WAREHOUSE:
      - STAGING
      - INTERMEDIATE
      - CORE
      - PUBLIC
```

### 2. Create the required Snowflake tag (once per database)
Add later

### 3. Run the macro for the desired environment
For development:
```bash
dbt run-operation create_schema_structure --args '{"role_name": "DBT"}' --target dev
```
This will create databases such as DH_RAW_DEV, DH_WAREHOUSE_DEV, and apply the tag orchestrated_by = 'dagster'.

For production:
```bash
dbt run-operation create_schema_structure --args '{"role_name": "DBT"}' --target prod
```
This will create DH_RAW, DH_WAREHOUSE, etc., and apply the tag orchestrated_by = 'dagster'.

### Notes
The macro is idempotent and can be safely re-run.

# Storage Integration & Stages
This project includes dbt macros to configure Snowflake storage integrations and external stages for accessing data in Azure Blob Storage.

## Storage Integration
Each environment has its own storage integration referencing the appropriate Azure Blob Storage account.

### Usage
#### For development:
```bash
dbt run-operation create_az_storage_integration --args '{integration_name: azure_int_dev, storage_allowed_locations: "azure://stgpjdatahubdev001.blob.core.windows.net/input-data", tenant_id: 01995f9e-cbf5-4ece-8340-a66e0985b5e7}'
```

#### For production:
```bash
dbt run-operation create_az_storage_integration --args '{integration_name: azure_int_prod, storage_allowed_locations: "azure://stgpjdatahubprod.blob.core.windows.net/input-data", tenant_id: 01995f9e-cbf5-4ece-8340-a66e0985b5e7}'
```

## Storage Integration IAM
The macro `create_az_storage_integration` creates the Snowflake storage integration object, but additional permissions must be granted manually in Azure to allow Snowflake access to the specified Blob Storage.

Specifically, the managed identity (or service principal) used by Snowflake must be assigned the "Storage Blob Data Reader" role on the storage container or account.

You can retrieve the identity using:
```sql
-- development
DESC STORAGE INTEGRATION azure_int_dev;

-- production
DESC STORAGE INTEGRATION azure_int_prod;
```

This provides the `AZURE_CLIENT_ID`, which must then be granted access in Azure using Azure CLI or the portal.

For detailed steps, refer to the Snowflake documentation:
[Grant Snowflake access to the storage locations (Step 2)](https://docs.snowflake.com/en/user-guide/data-load-azure-config#step-2-grant-snowflake-access-to-the-storage-locations)

## External Stages
External stages are used to reference files in Azure Blob Storage via a Snowflake `STAGE` object. To enable this, a corresponding `FILE FORMAT` must exist and be referenced during stage creation.

### Prerequisite
Before creating a stage, the required file format (e.g. `parquet_format`) must be created in the corresponding database and schema. This ensures consistency and reuse when defining external stages and external tables.

### Create File Format
The following command creates a reusable file format for reading Parquet files:

#### For development:
```bash
dbt run-operation create_parquet_file_format --target dev
```

#### For production:
```bash
dbt run-operation create_parquet_file_format --target prod
```

### Create a single stage (explicit parameters)
#### For development:
```bash
dbt run-operation create_ext_stages \
  --args '{db: dh_raw_dev, schema: source, stage_name: azdevstage, url: "azure://stgpjdatahubdev001.blob.core.windows.net/input-data", storage_integration: azure_int_dev, file_format: "source.parquet_format"}'
```

#### For production:
```bash
dbt run-operation create_ext_stages \
  --args '{db: dh_raw, schema: source, stage_name: azprodstage, url: "azure://stgpjdatahubprod.blob.core.windows.net/input-data", storage_integration: azure_int_prod, file_format: "source.parquet_format"}'
```

## External Tables
External tables allow you to query data stored in external systems (such as Azure Blob Storage) directly, without ingesting the data into Snowflake.
In this project, external tables are managed using the [dbt-external-tables package](https://hub.getdbt.com/dbt-labs/dbt_external_tables/latest/).

### Usage - create if missing and refresh metadata

To ___create and fresh___ all configured external tables, run:

#### For development
```bash
dbt run-operation stage_external_sources --target dev

# stage a particular external source table:
dbt run-operation stage_external_sources --args "select: OPEN_METEO_EXT_RAW.raw_forecast_hourly" --target dev
```

#### For production
```bash
dbt run-operation stage_external_sources --target prod
```

### Usage - create/ replace and refresh metadata

To ___create/ replace and refresh___ all configured external tables, run:

#### For development
```bash
dbt run-operation stage_external_sources --vars "ext_full_refresh: true" --target dev
```

#### For production
```bash
dbt run-operation stage_external_sources --vars "ext_full_refresh: true" --target prod
```

### Notes
- The macro will infer the schema of Parquet files using `INFER_SCHEMA` and automatically create the corresponding `EXTERNAL TABLE` in Snowflake.
- The stage must exist and point to a valid location with accessible files.
- The `FILE FORMAT` must be correctly referenced in your sources.yml (e.g., `source.parquet_format`).
- Make sure a valid warehouse is set in your profiles.yml, or explicitly selected in the macro if required.
- If permission errors occur, ensure the storage integration is properly granted access via Azure IAM (see section: `Storage Integration IAM`).

### Note on pseudocolumns in External Tables
Snowflake external tables expose special metadata columns that are **not visible** when using `SELECT *` or describing the table, but are nonetheless available for querying.

These pseudocolumns include:
- `METADATA$FILENAME`
- `METADATA$FILE_ROW_NUMBER`
- `METADATA$FILE_LAST_MODIFIED`

For example, you can explicitly query them like this:

```sql
SELECT
  metadata$filename       AS file_name,
  metadata$file_row_number AS row_num,
  METADATA$FILE_LAST_MODIFIED AS last_modified,
  value
FROM DH_RAW_DEV.OPEN_METEO.STG_MKA;
```

---
