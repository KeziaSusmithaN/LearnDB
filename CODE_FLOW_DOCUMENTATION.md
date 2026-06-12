# Home Delivery Data Processing - Code Flow Documentation

## Overview
This document outlines the complete data flow for Home Delivery data processing through multiple pipeline stages, from data ingestion to final Synapse database population.

---

## Table of Contents
1. [Pipeline Stages](#pipeline-stages)
2. [Data Flow Diagram](#data-flow-diagram)
3. [Detailed Stage Descriptions](#detailed-stage-descriptions)
4. [Key Validations and Checks](#key-validations-and-checks)
5. [Important Observations](#important-observations)

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

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 1: PREPROCESSING (PL_HOMEDELIVERY_DATA_PREPROCESSING)     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Exchange (ADLS)                     Commdl01 (ADLS)             │
│  ┌──────────────────────┐    ┌───────────────────────────────┐  │
│  │ HomeDelivery_Xref    │───▶│ Source/HomeDelivery_Xref_Data │  │
│  │ HomeDelivery_Data    │    └───────────────────────────────┘  │
│  │ (Inbound)            │                                        │
│  └──────────────────────┘    ┌───────────────────���───────────┐  │
│           │                  │ Preprocess/HomeDelivery_Data   │  │
│           └─────────────────▶│ (Preprocessing Notebook)       │  │
│                              │ • Header normalization         │  │
│                              │ • CRM_ID enrichment (via xref) │  │
│                              └───────────────────────────────┘  │
│                                         │                       │
│                                         ▼                       │
│                              ┌───────────────────────────────┐  │
│                              │ Postprocess/HomeDelivery_Data │  │
│                              │ (Synapse Load)                │  │
│                              └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 2: SOURCE TO RAW (PL_GLOBALREBATES_MASTER_PULL_SRC_RAW)   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Copy from Postprocess to Source                         │    │
│  │ Source/HomeDelivery_Data ──▶ Raw/source/homedelivery_data    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Archive processed files                                 │    │
│  │ Source/HomeDelivery_Data ──▶ Source/Archive/Home...    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 3: VALIDATION & TRANSFORMATION                            │
│ (PL_GLOBALREBATES_MASTER_DI_DQ_DC_SRV_SRS)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  source2raw_master_package.scala                                │
│  • Read input source file                                       │
│  • Column name & count validation                               │
│  • Write new records to Raw table (EXCEPT clause)               │
│                      │                                          │
│                      ▼                                          │
│  raw2stage_master_package.scala                                 │
│  • Trim service                                                 │
│  • Datatype validation service                                  │
│  • Data quality service                                         │
│  • Write new records to Stage table (EXCEPT clause)             │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 4: CDC & CURATION                                         │
│ (PL_GLOBALREBATES_MASTER_CDC_CUR)                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  stage2curated2syn_Ecom_master_package.scala                    │
│  • CDC Indicator: NOT ENABLED for Home Delivery                 │
│  • Write current file data to Curated layer                     │
│  • Write current file data to temp location (for Synapse)       │
│  • Delete files from Raw/Source folder                          │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ STAGE 5: SYNAPSE REFRESH                                        │
│ (PL_GLOBALREBATES_MASTER_SYNP_RFR)                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  • Merge parquet files from previous step                       │
│  • Execute: usp_RefreshSynapseFromAdls                          │
│  • Push latest data into dedicated pool table                   │
│  • Target: [CommGlobalRebates].[homedelivery_data]              │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Detailed Stage Descriptions

### **STAGE 1: Data Preprocessing** (`PL_HOMEDELIVERY_DATA_PREPROCESSING`)

**Purpose:** Prepare and enrich raw data files before processing

**Steps:**

1. **Copy Cross-Reference File**
   - **Source:** `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/<filename>`
   - **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Xref/Source/HomeDelivery_Xref_Data/<filename>`
   - **Note:** No specific filename format defined

2. **Copy Data File**
   - **Source:** `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Data/Inbound/<filename>`
   - **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/Preprocess/HomeDelivery_Data/<filename>`
   - **Action:** Delete file from ecrmsa01 after copying

3. **Preprocessing Notebook Execution**
   - **Location:** `/Workspace/COMM - ElancoCORE-Global Rebates (GRBS)/Services/HomeDelivery_Preprocess`
   
   **Logic:**
   - **Duplicate Check:** If file was previously processed:
     - Delete data from Raw/Stage/Curated tables for this filename
     - Delete data from all log tables for this filename
   
   - **Data Transformation:**
     - Normalize column headers to corrected names per source specifications
     - Join with `homedelivery_xref_data` to enrich with CRM_ID
   
   - **Output:** Write processed data to `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Postprocess/HomeDelivery_Data/`

4. **Database Operations**
   - **Delete from Synapse:** `commglobalrebates.homedelivery_data`
   - **Copy to Source Folder:** Move from Postprocess to Source location
   - **Cleanup:** Delete data from Exchange/Preprocess folders

---

### **STAGE 2: Source to Raw Layer** (`PL_GLOBALREBATES_MASTER_PULL_SRC_RAW`)

**Purpose:** Migrate validated data to Raw layer and maintain source archive

**Steps:**

1. **Source to Raw Copy**
   - **Source:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data`
   - **Destination:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Raw/source/homedelivery_data`

2. **Archive Source Files**
   - **Source:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data`
   - **Archive Location:** `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/Archive/HomeDelivery_Data`
   - **Purpose:** Maintain historical copy for audit trail

---

### **STAGE 3: Data Integration, Quality & Validation** (`PL_GLOBALREBATES_MASTER_DI_DQ_DC_SRV_SRS`)

**Purpose:** Validate data quality and apply business rules transformations

**Component 1: Source to Raw (`source2raw_master_package.scala`)**

Operations:
- Read input source file from Raw layer
- **Validations:**
  - Column name validation (verify expected columns exist)
  - Column count validation (verify expected number of columns)
- Write new records to Raw table
  - Uses EXCEPT clause to identify new records only
  - Avoids duplicate processing

**Component 2: Raw to Stage (`raw2stage_master_package.scala`)**

Operations:
- **Trim Service:** Remove leading/trailing whitespace from string columns
- **Datatype Service:** Validate and convert columns to correct data types
- **Data Quality Service:** Apply quality checks per DIDQ specifications
- Write new records to Stage table
  - Uses EXCEPT clause for delta processing
  - Only new/modified records processed

---

### **STAGE 4: CDC & Curation** (`PL_GLOBALREBATES_MASTER_CDC_CUR`)

**Purpose:** Apply Change Data Capture (CDC) logic and prepare final curated dataset

**Component: `stage2curated2syn_Ecom_master_package.scala`**

**CDC Processing:**
- **Status:** CDC Indicator NOT ENABLED for Home Delivery data
- This means full refresh approach (all records processed each run)

**Curation Steps:**
1. Write current file data to **Curated layer**
   - Final business-ready dataset
   
2. Write current file data to **Temporary location**
   - Staging for Synapse load in next stage
   
3. **Cleanup Operations:**
   - Delete processed files from Raw layer
   - Delete processed files from Source layer
   - Maintains clean ADLS structure

---

### **STAGE 5: Synapse Refresh** (`PL_GLOBALREBATES_MASTER_SYNP_RFR`)

**Purpose:** Load final curated data into Synapse dedicated SQL pool

**Steps:**

1. **Merge Parquet Files**
   - Consolidate parquet files from previous CDC/Curated stage
   - Creates single unified dataset for load

2. **Execute Synapse Refresh Procedure**
   - **Procedure:** `[CommGlobalRebates].[usp_RefreshSynapseFromAdls]`
   - **Target Table:** `[CommGlobalRebates].[homedelivery_data]`
   - Pushes merged data into dedicated pool table
   - Makes data available for analytics and reporting

---

## Key Validations and Checks

### **Column Validations**
- Column name validation against metadata
- Column count validation
- Data type enforcement

### **Data Quality Services**
- Trim whitespace from string values
- Type conversion services
- Data quality rule application

### **Duplicate Prevention**
- EXCEPT clause usage in Scala packages prevents duplicate record writes
- Historical data deletion before reprocessing prevents duplication

### **Duplicate File Handling**
- If a file is reprocessed:
  1. Delete all data for that filename from Raw/Stage/Curated tables
  2. Delete all related log entries
  3. Reprocess cleanly from source

---

## Important Observations

### **Data Type Constraints**
- **String Columns:** All Home Delivery columns are stored as STRING data type
  - **Exception:** `sold` and `amount` columns (numeric)
  - **Impact:** Date columns do not have referential integrity checks
  - Dates stored as strings require explicit parsing during consumption

### **Primary Key Definition**
- **No Primary Key Metadata:** Home Delivery dataset lacks primary key definition
- **Implication:** 
  - Uniqueness enforcement not possible at data layer
  - Business logic must handle potential duplicates
  - De-duplication should occur at consumption layer if needed

### **Data Quality Checks**
- **NULL Validations:** NOT NULL checks are currently **COMMENTED OUT**
- **Status:** Requires review and potential enablement
- **Recommendation:** Evaluate if NULL values are acceptable for each column

### **CDC Status**
- **Change Data Capture:** NOT ENABLED for Home Delivery
- **Processing Mode:** Full refresh approach
- **Performance:** All records processed regardless of changes
- **Consideration:** Evaluate CDC enablement for performance optimization

---

## Data Storage Locations Reference

| Layer | Location |
|-------|----------|
| **Source (Exchange)** | `https://bpaze1iecrmsa01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Data/Inbound/` |
| **Preprocessing** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/Preprocess/HomeDelivery_Data/` |
| **Postprocessing** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery_Postprocess/HomeDelivery_Data/` |
| **Source (Commdl01)** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/HomeDelivery_Data/` |
| **Raw** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Raw/source/homedelivery_data/` |
| **Stage** | (Synapse internal table) |
| **Curated** | (Synapse internal table) |
| **Archive** | `https://bpaze1icommdl01.dfs.core.windows.net/comm-globalrebates/HomeDelivery/Source/Archive/HomeDelivery_Data/` |

---

## Summary

The Home Delivery data processing pipeline is a **5-stage ETL process** that:

1. ✅ **Preprocesses** raw data with cross-reference enrichment
2. ✅ **Migrates** validated data to Raw layer with archival
3. ✅ **Validates** data quality and applies transformations
4. ✅ **Curates** data with CDC awareness and cleanup
5. ✅ **Loads** final dataset into Synapse for consumption

**Key takeaway:** Data flows from Exchange ADLS through preprocessing, validation, and curation layers before final Synapse population, with complete audit trails maintained through archival mechanisms.

---

*Last Updated: 2026-06-12*
*Document Version: 1.0*
