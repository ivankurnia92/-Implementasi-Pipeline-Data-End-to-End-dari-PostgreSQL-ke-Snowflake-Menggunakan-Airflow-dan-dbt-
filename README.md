# 🚀 Northwind Modern Data Pipeline (PostgreSQL → Snowflake → BI)

> End-to-end **Modern Data Stack** project implementing **ELT (Extract, Load, Transform)** using the Northwind dataset — from OLTP system to analytics-ready warehouse with **Star Schema** and BI dashboarding.

![Pipeline](https://img.shields.io/badge/Pipeline-ELT-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.7.1-green) ![dbt](https://img.shields.io/badge/dbt-1.7.19-orange) ![Snowflake](https://img.shields.io/badge/Snowflake-✓-lightblue) ![Docker](https://img.shields.io/badge/Docker-✓-blue)

---

## 📌 Project Overview

This project demonstrates how to build a **production-style data pipeline** using modern tools:

* Extract data from **PostgreSQL (OLTP)**
* Load into **Snowflake (Data Warehouse)**
* Transform using **dbt (staging → marts)**
* Orchestrate using **Apache Airflow**
* (Optional) Visualize using **Metabase**

All components run inside **Docker**, ensuring reproducibility and easy setup.

---

## 🏗️ Architecture

```text
PostgreSQL (OLTP)
        ↓
Airflow (Orchestration)
        ↓
Snowflake (Raw Layer)
        ↓
dbt (Transformation: Staging → Marts)
        ↓
BI Layer (Metabase)
```

---

## 🛠️ Tech Stack

| Layer          | Technology           | Description                       |
| -------------- | -------------------- | --------------------------------- |
| Source         | PostgreSQL 15        | OLTP database                     |
| Orchestration  | Apache Airflow 2.7.1 | DAG scheduling & pipeline control |
| Warehouse      | Snowflake            | Cloud Data Warehouse              |
| Transformation | dbt Core 1.7.19      | SQL-based transformation          |
| Data Load      | write_pandas         | High-performance ingestion        |
| Visualization  | Metabase (optional)  | Dashboard & BI                    |
| Infra          | Docker & Compose     | Containerized environment         |
| Language       | Python 3.12          | Pipeline scripting                |

---

## 📂 Project Structure

```bash
.
├── dags/                        # Airflow DAGs
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # Data cleaning & standardization
│   │   └── marts/              # Fact & dimension tables
│   ├── snapshots/              # SCD Type 2 tracking
│   ├── macros/                 # Custom dbt macros
│   ├── dbt_project.yml
│   └── profiles.yml
├── init_sql/                   # PostgreSQL initialization
├── logs/                       # Airflow logs
├── plugins/                    # Airflow plugins
├── docker-compose.yml
├── Dockerfile
└── .env                        # Credentials (ignored)
```

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone https://github.com/username/repo.git
cd repo
```

---

### 2. Setup Environment Variables

Create `.env` file:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DBT_DEV_DB
SNOWFLAKE_SCHEMA=PUBLIC_1_RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

---

### 3. Run Services

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
```

---

### 4. Access Services

| Service    | URL                                            |
| ---------- | ---------------------------------------------- |
| Airflow    | [http://localhost:8080](http://localhost:8080) |
| Metabase   | [http://localhost:3000](http://localhost:3000) |
| PostgreSQL | localhost:5433                                 |

---

## ⚙️ Airflow Setup

Create connections:

```bash
# PostgreSQL
airflow connections add postgres_northwind \
  --conn-type postgres \
  --conn-host host.docker.internal \
  --conn-port 5433 \
  --conn-login airflow \
  --conn-password airflow \
  --conn-schema northwind

# Snowflake
airflow connections add snowflake_conn \
  --conn-type snowflake \
  --conn-login your_user \
  --conn-password your_password
```

---

## 📊 Data Pipeline

### Ingestion (Airflow)

* Extract from PostgreSQL
* Load to Snowflake using `write_pandas`

#### Tables Ingested

* Customers
* Orders
* Order Details
* Products
* Inventory Transactions
* Purchase Orders

### Features

* Auto table creation
* Data cleaning (NULL handling)
* Retry mechanism
* Logging

---

## 🧹 Data Transformation (dbt)

### Layers

```text
Raw → Staging → Marts
```

### Commands

```bash
dbt run
dbt test
dbt snapshot
dbt docs generate
```

---

## 🧠 Data Modeling

Implements **Star Schema**:

* Fact Tables: `fact_sales`, `fact_inventory`
* Dimension Tables: `dim_customer`, `dim_product`

---

## 📸 Snapshot (SCD Type 2)

Tracks historical changes using:

* `dbt_valid_from`
* `dbt_valid_to`

---

## 📈 BI & Dashboard (Metabase)

Example dashboards:

* Sales Performance
* Inventory Monitoring
* Supply Chain Efficiency

---

## 🔥 Key Insights

* Revenue trends & growth
* Inventory health (low stock, dead stock)
* Purchase vs sales efficiency
* Cash flow from inventory

---

## 🎯 Future Improvements

* Add data quality monitoring
* Implement CI/CD for dbt
* Add forecasting models
* Optimize warehouse cost (Snowflake)

---

## 👨‍💻 Author

Built as part of a **Data Engineering / Analytics Engineering Portfolio Project**.

---

## ⭐️ Support

If you find this project useful, consider giving it a ⭐ on GitHub!
