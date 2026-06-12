# Home Delivery Data Processing - Code Flow Documentation

## Overview
This document outlines the complete data flow for Home Delivery data processing through multiple pipeline stages, from data ingestion to final Synapse database population.

---

## Table of Contents
1. [Pipeline Stages](#pipeline-stages)
2. [Detailed Stage Descriptions](#detailed-stage-descriptions)
3. [Key Validations and Checks](#key-validations-and-checks)
4. [Important Observations](#important-observations)
5. [Data Storage Locations Reference](#data-storage-locations-reference)
6. [Summary](#summary)

---

## Pipeline Stages

The data processing is divided into **5 main pipeline stages**:

| Stage | Pipeline Code | Purpose |
|-------|---------------|---------|
| 1 | `PL_HOMEDELIVERY_DATA_PREPROCESSING` | Data Preprocessing & Enrichment |
| 2 | `PL_GLOBALREBATES_MASTER_PULL_SRC_RAW` | Source to Raw Layer Migration |
| 3 | `PL_GLOBALREBATES_MASTER_DI_DQ_DC_SRV_SRS` | Data Validation & Transformation |
| 4 | `PL_GLOBALREBATES_MASTER_CDC_CUR` | CDC Processing & Curation |
| 5 | `PL_GLOBALREBATES_MASTER_SYNP_RFR` | Synapse Refresh & Final Load |

---

## Detailed Stage Descriptions

### **STAGE 1: Data Preprocessing** (`PL_HOMEDELIVERY_DATA_PREPROCESSING`)

**Purpose:** Prepare and enrich raw data files before processing.

#### 1. Copy Cross-Reference File
- **Source:** `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/<filename>`
- **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/Source/HomeDelivery_Xref_Data/<filename>`
- **Note:** No filename format is defined.
- **Additional action:** Delete file from `ecrmsa01` after copy.

#### 2. Copy Data File
- **Source:** `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Data/Inbound/<filename>`
- **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/Preprocess/HomeDelivery_Data/<filename>`

#### 3. Preprocessing Notebook
- **Notebook path:** `/Workspace/COMM - ElancoCORE-Global Rebates (GRBS)/Services/HomeDelivery_Preprocess`

**Logic:**
- If the file was processed previously:
  1. Delete the file's data from Raw, Stage, and Curated tables.
  2. Delete the file's data from all log tables.
- Convert column headers to corrected names for each source.
- Join with `homedelivery_xref_data` and derive `CRM_ID`.
- Write processed data into:
  `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Postprocess/HomeDelivery_Data/`

#### 4. Post-Processing Actions
- **Delete data from Synapse table:** `commglobalrebates.homedelivery_data`
- **Copy data file from Postprocess to Source folder:**
  - From: `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Postprocess/HomeDelivery_Data/`
  - To: `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data`
- **Cleanup:** Delete data from exchange and preprocess folders.

---

### **STAGE 2: Source to Raw Layer** (`PL_GLOBALREBATES_MASTER_PULL_SRC_RAW`)

**Purpose:** Copy processed source data into raw layer and maintain archive.

#### 1. Copy from Source to Raw
- **Source:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data`
- **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Raw/source/homedelivery_data`

#### 2. Archive Source Folder
- **Source:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data`
- **Archive location:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/Archive/HomeDelivery_Data`

---

### **STAGE 3: Data Integration, Quality & Validation** (`PL_GLOBALREBATES_MASTER_DI_DQ_DC_SRV_SRS`)

**Purpose:** Validate source structure and apply DIDQ rules before stage load.

#### Component 1: `source2raw_master_package.scala`
- Read input source file.
- Perform column name validation.
- Perform column count validation.
- Write new records to raw table using EXCEPT clause.
- **Raw table:** `homedelivery_raw.homedelivery_data`

#### Component 2: `raw2stage_master_package.scala`
- Read data from raw table.
- Filter for current file.
- Apply DIDQ validations:
  - Trim service
  - Datatype service
  - Data quality service
- Write records to stage table.
- **Stage table:** `homedelivery_stage.homedelivery_data`

---

### **STAGE 4: CDC & Curation** (`PL_GLOBALREBATES_MASTER_CDC_CUR`)

**Purpose:** Curate current file data and prepare Synapse-ready output.

#### Component: `stage2curated2syn_Ecom_master_package.scala`
- Read data from stage table and apply CDC if enabled.
- Write current file data to curated layer.
- **Curated table:** `homedelivery_curated.homedelivery_data`
- Write current file data to a temp location used for Synapse dedicated pool load.
- Delete files from Raw and Source folders.

**Observation:** Home Delivery does not have CDC enabled.

---

### **STAGE 5: Synapse Refresh** (`PL_GLOBALREBATES_MASTER_SYNP_RFR`)

**Purpose:** Push latest data into Synapse dedicated pool.

#### Stored Procedure
- **Procedure:** `[CommGlobalRebates].[usp_RefreshSynapseFromAdls]`
- **Action:** Push latest data into dedicated pool table.
- **Target table:** `commglobalrebates.homedelivery_data`

---

## Key Validations and Checks

### **Column and Structural Validations**
- Column name validation
- Column count validation
- Datatype validation

### **Data Quality Services**
- Trim service
- Datatype service
- Data quality service

### **Duplicate Handling**
- EXCEPT clause is used to write only new records.
- Reprocessed files are cleaned from Raw, Stage, Curated, and log tables before rerun.

---

## Important Observations

### **Datatype Observations**
- As per Home Delivery metadata, all columns are strings except `sold` and `amount`.
- Therefore, date columns do not currently have datatype integrity.

### **Primary Key Metadata**
- No primary key metadata is set for Home Delivery data.

### **NULL Checks**
- NOT NULL checks are commented out.

### **CDC Status**
- CDC indicator is not enabled for Home Delivery.

---

## Data Storage Locations Reference

| Layer | Location |
|-------|----------|
| **Xref Exchange** | `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/<filename>` |
| **Xref Commdl01 Source** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/Source/HomeDelivery_Xref_Data/<filename>` |
| **Inbound Source Data** | `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Data/Inbound/<filename>` |
| **Preprocess** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/Preprocess/HomeDelivery_Data/<filename>` |
| **Postprocess** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Postprocess/HomeDelivery_Data/` |
| **Source** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data` |
| **Raw** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Raw/source/homedelivery_data` |
| **Archive** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/Archive/HomeDelivery_Data` |
| **Synapse Table** | `commglobalrebates.homedelivery_data` |

---

## Summary

The Home Delivery data processing flow is a **5-stage ETL pipeline**:

1. **Preprocess** xref and inbound source files.
2. **Move** processed data into source and raw layers.
3. **Validate** structure and apply DIDQ transformations.
4. **Curate** current file data and prepare Synapse-ready output.
5. **Refresh** Synapse dedicated pool with the latest data.

**Key takeaway:** Home Delivery follows a full-refresh style pipeline with preprocessing, source-to-raw movement, DIDQ validation, curated output generation, and Synapse load. CDC is currently not enabled, and key metadata constraints such as primary key and NOT NULL enforcement are limited.

---

*Last Updated: 2026-06-12*  
*Document Version: 1.1*
