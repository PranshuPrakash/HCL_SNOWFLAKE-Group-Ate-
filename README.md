# 📊 Telecom Data Pipeline – Snowflake Project

## 📌 Overview

This project implements an end-to-end **data pipeline for telecom Call Detail Records (CDR)** using Snowflake.
It includes data ingestion, transformation, enrichment, analytics, monitoring, and alerting.

---

## 🧱 Architecture

```
CDR (Raw Data)
      ↓
STG_CDR (Cleaning & Transformation)
      ↓
ENRICHED_CDR (Join with Customer & Tower)
      ↓
FACT_CDR (Final Analytics Table)
      ↓
Dashboard + Alerts + Audit Logs
```

---

## 📂 Source Tables

* **CDR** → Raw call records
* **CUSTOMER** → Customer details (name, plan)
* **TOWER** → Tower location details (region, city)

---

## ⚙️ ETL Process

### 🔹 1. Extract

* Data is loaded into Snowflake tables via UI.

---

### 🔹 2. Transform (STG_CDR)

* Removed duplicates using `ROW_NUMBER()`
* Handled null values using `COALESCE`
* Standardized timestamp format
* Added derived columns:

  * `Revenue = Duration × 0.02`
  * `IsInternational = 1 if CallType = 'INT'`

---

### 🔹 3. Enrich (ENRICHED_CDR)

* Joined with:

  * CUSTOMER (for Caller & Receiver)
  * TOWER (for location details)
* Used `LEFT JOIN` to preserve all call records

---

### 🔹 4. Load (FACT_CDR)

* Final table created for analytics
* Used `MERGE` to:

  * Insert new records
  * Update existing records
* Added:

  * Surrogate key (`CDR_Key`)
  * `CallDate` for analysis

---

## 📊 Dashboard

### View: `VW_CUSTOMER_USAGE`

Provides aggregated metrics:

* Total Calls
* Total Duration
* Total Revenue
* Average Call Duration
* International Calls

Used for visualization in tools like Power BI / Tableau.

---

## 🚨 Alerts

### High Usage Detection

* Created `HIGH_USAGE_ALERT` table
* Identifies customers with high total duration

```
HAVING SUM(Duration) > 500
```

### Email Notification

* Configured Snowflake ALERT
* Sends email when high usage detected

---

## 🧪 Validation Checks

Ensures data quality:

* Invalid phone numbers (not 10 digits)
* Negative duration
* Null CallID

---

## 📜 Audit Logging

### Table: `LOAD_AUDIT_LOG`

Tracks:

* Total records processed
* Successful records
* Failed records
* Load timestamp

Used for monitoring pipeline health.

---

## 🧠 Key Concepts Used

* Window Functions (`ROW_NUMBER`)
* Data Cleaning & Deduplication
* Joins (Fact + Dimension)
* Incremental Loading (`MERGE`)
* Data Quality Checks
* Alerting & Monitoring
* Star Schema Design

---

## 🚀 How to Run

1. Ensure tables exist in Snowflake:

   * CDR, CUSTOMER, TOWER
2. Run SQL script:

   * Create STG_CDR
   * Create ENRICHED_CDR
   * Create FACT_CDR
   * Run MERGE
3. Create:

   * Dashboard View
   * Alert Table
   * Audit Logs

---

## 💡 Future Enhancements

* Automate pipeline using scheduling (Tasks)
* Integrate with external alerting systems
* Add more validation rules
* Partition FACT table for performance

---

## 🏁 Conclusion

This project demonstrates a **complete production-style data pipeline**, including:

* Data engineering
* Data quality
* Monitoring
* Analytics

---

**Group:** Group Ate
**Domain:** Data Engineering / Analytics
