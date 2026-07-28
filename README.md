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

**Objective:** consolidate customer, order, product, seller, payment,
review, and logistics data into a single source of truth that answers
questions across three subject areas — **revenue performance**, **customer
behavior**, and **delivery/logistics performance**.

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
