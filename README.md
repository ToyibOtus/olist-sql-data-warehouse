# Olist SQL Data Warehouse

End-to-end data warehouse built in Microsoft SQL Server on Olist's Brazilian
e-commerce dataset, using Medallion architecture, Kimball-style dimensional
modeling, and T-SQL-only ETL — from raw ingestion through business-ready
star schema, exploratory and advanced analytics, and Excel/Tableau
dashboards.

---

## Project Overview

This project simulates building a production-grade analytics warehouse for
an e-commerce marketplace, covering the full lifecycle: raw data ingestion,
data quality resolution, dimensional modeling, and analytical reporting —
entirely in T-SQL, with no external orchestration or scripting language.

---

## Project Requirements

**Purpose:** Build a modern SQL Server data warehouse that consolidates
Olist's e-commerce marketplace data into a single source of truth,
enabling analytical and BI reporting for data-informed decision-making.

**Data Source:** 9 relational CSV files from the Olist marketplace
platform (Brazilian e-commerce), a one-time historical extract — not a
live connection.

**Data Quality:** Issues are identified and resolved after ingestion,
before integration — Bronze preserves raw data as-is, Silver applies
documented cleaning rules, Gold consumes only trusted data.

**Data Integration:** Cleaned data is consolidated via conformed
dimensions and a bus matrix of fact tables, exposed through Gold-layer
views for fast, easy analytical access.

**Documentation:** Data dictionary, naming conventions, and a
non-technical data catalog are maintained in `/docs`.

---

## Data Engineering & Data Analytics

This project is split into two disciplines, each with its own objective:

### Data Engineering
**Objective:** design and build an industry-standard SQL Server data
warehouse — with layered Medallion architecture, documented data quality
rules, and a standardized logging/audit system — that makes data easy to
access, trust, and analyze.

### Data Analytics
**Objective:** use the Gold-layer warehouse to answer real business
questions, split into:
- **Exploratory Data Analytics** — profiling and sanity-checking the data
- **Advanced Data Analytics** — RFM segmentation, cohort analysis,
  delivery performance, seller scorecards, and Pareto analysis, all in
  T-SQL

---

## Project Roadmap

* Architecture design and repository setup
* Bronze layer — DDL and ETL procedures
* Silver layer — DDL, ETL procedures, and quality checks
* Gold layer — DDL, ETL procedures, views, and integration checks
* Orchestration layer — Bronze, Silver, Gold, and master pipelines
* Documentation — architecture diagrams, data dictionary, naming conventions

---

## Tools & Technologies

| Tool | Purpose |
|---|---|
| Microsoft SQL Server | Database engine and primary implementation environment |
| SQL Server Management Studio (SSMS) | Query development and execution interface |
| T-SQL | ETL scripting, stored procedures, data modelling |
| Draw.io | Architecture, data flow, integration, and data model diagrams |
| GitHub | Version control and portfolio hosting |
| Notion | Project planning and task management |

---

## License

This project is licensed under the **MIT License**. You are free to use,
modify, or share with proper attribution.

---

## About Me

Hi there! I am **Otusanya Toyib Oluwatimilehin**, an aspiring Data Analyst
passionate about building reliable data pipelines, well-structured data
models, and analytically powerful data warehouses.

📧 toyibotusanya@gmail.com
📞 07082154436
