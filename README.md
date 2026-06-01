# NYC Yellow Taxi End-to-End Data Engineering Project

This repository contains an end-to-end data engineering pipeline designed to ingest, transform, and analyze the famous New York City Yellow Taxi dataset. The project demonstrates modern data stack practices, focusing heavily on robust data transformation and modeling layers.

## 📌 Project Architecture

The pipeline is structured to move data from raw, messy ingestion formats into clean, analytics-ready data models optimized for Business Intelligence (BI) and reporting.

* **Data Ingestion & Orchestration:** Raw NYC Yellow Taxi trip data handling.
* **Data Transformation:** High-performance processing using Jupyter Notebooks (`data_transformation/`).
* **Data Modeling & Warehousing:** Structured SQL data modeling using **dbt** (`nycyellowtaxi_dbt/`) following dimensional modeling (Star Schema) design patterns.

---

## 📁 Repository Structure

```text
├── data_transformation/    # Notebooks for initial ETL, EDA, and data cleaning
├── nycyellowtaxi_dbt/      # dbt (Data Build Tool) project for data warehousing
│   ├── models/             # Staging, Intermediate, and Mart models
│   ├── dbt_project.yml     # dbt configuration
│   └── profiles.yml        # Database connection profiles
└── README.md               # Project documentation
