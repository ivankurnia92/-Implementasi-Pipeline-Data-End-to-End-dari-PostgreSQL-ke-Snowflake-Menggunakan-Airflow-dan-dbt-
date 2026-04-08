# 🚀 Northwind Data Pipeline: End-to-End PostgreSQL → Snowflake

> Pipeline data end-to-end modern yang mengimplementasikan arsitektur **ELT (Extract, Load, Transform)** menggunakan dataset **Northwind Traders** — dari database operasional hingga data warehouse siap analitik dengan Star Schema.

![Pipeline](https://img.shields.io/badge/Pipeline-ELT-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.7.1-green) ![dbt](https://img.shields.io/badge/dbt-1.7.19-orange) ![Snowflake](https://img.shields.io/badge/Snowflake-✓-lightblue) ![Docker](https://img.shields.io/badge/Docker-✓-blue)

---

## 📌 Deskripsi Proyek

Proyek ini mendemonstrasikan implementasi pipeline data modern secara end-to-end dengan memindahkan data dari database operasional **(PostgreSQL)** ke data warehouse **(Snowflake)**. Apache Airflow digunakan sebagai orkestrator pipeline, sedangkan **dbt** menangani transformasi data dari raw layer hingga menjadi **Star Schema** yang siap digunakan untuk analitik bisnis.

Seluruh infrastruktur berjalan di atas **Docker** untuk memastikan lingkungan yang terisolasi, reproducible, dan mudah dijalankan di berbagai environment.

---

## 🏗️ Arsitektur Sistem

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          DOCKER ENVIRONMENT                              │
│                                                                          │
│  ┌─────────────────┐    ┌──────────────────────────────────────────┐    │
│  │   PostgreSQL 15  │    │           Apache Airflow 2.7.1           │    │
│  │  (Source/OLTP)   │───▶│                                          │    │
│  │                  │    │  DAG: northwind_ingestion                │    │
│  │  - Northwind DB  │    │  ┌──────────────────────────────────┐   │    │
│  │  - 9 Tables      │    │  │  PostgresHook → write_pandas      │   │    │
│  └─────────────────┘    │  │  (Bulk Ingest / LocalExecutor)   │   │    │
│                          │  └──────────────────────────────────┘   │    │
│                          └──────────────────────┬───────────────────┘    │
└─────────────────────────────────────────────────┼────────────────────────┘
                                                   │
                                                   ▼
                             ┌─────────────────────────────────────┐
                             │             SNOWFLAKE                │
                             │         (DBT_DEV_DB)                 │
                             │                                      │
                             │  PUBLIC_1_RAW  ──▶  dbt run         │
                             │  (Raw Tables)        │               │
                             │                      ▼               │
                             │              STAGING Layer           │
                             │              (Cleaning &             │
                             │               Standarisasi)          │
                             │                      │               │
                             │                      ▼               │
                             │               MARTS Layer            │
                             │           (Star Schema: Fact         │
                             │            & Dimension Tables)       │
                             │                                      │
                             │  + dbt snapshot (SCD Type 2)        │
                             └─────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Komponen | Teknologi | Keterangan |
|---|---|---|
| **Source Database** | PostgreSQL 15 | OLTP, running in Docker |
| **Orchestrator** | Apache Airflow 2.7.1 | LocalExecutor, Docker |
| **Data Warehouse** | Snowflake | Target OLAP (`DBT_DEV_DB`) |
| **Transformation** | dbt Core 1.7.19 | Staging, Marts, Snapshots |
| **Data Movement** | `write_pandas` (Snowflake) | Bulk ingest high-performance |
| **Infrastruktur** | Docker & Docker Compose | Isolated environment |
| **Language** | Python 3.12 | DAG scripting |

---

## 📂 Struktur Direktori

```
.
├── dags/
│   └── northwind_ingestion_cdc.py   # DAG ingestion Postgres → Snowflake
├── dbt_project/
│   ├── models/
│   │   ├── staging/                 # Cleaning & standarisasi kolom
│   │   └── marts/                   # Star Schema (Fact & Dimension)
│   ├── snapshots/                   # SCD Type 2 tracking perubahan data
│   ├── macros/
│   │   └── generate_schema_name.sql # Override schema naming dbt
│   ├── dbt_project.yml              # Konfigurasi project dbt
│   └── profiles.yml                 # Koneksi dbt ke Snowflake
├── init_sql/
│   └── setup.sql                    # Inisialisasi DB 'northwind' di Postgres
├── logs/                            # Airflow task logs
├── plugins/                         # Airflow custom plugins
├── .env                             # Kredensial Snowflake (tidak di-commit)
├── Dockerfile                       # Custom image (Postgres + Snowflake libs)
├── docker-compose.yml               # Konfigurasi seluruh layanan
└── README.md                        # Dokumentasi proyek
```

---

## 🚀 Cara Menjalankan Proyek

### Prasyarat

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) terinstall & berjalan
- Akun [Snowflake](https://snowflake.com) aktif
- Git terinstall

### 1. Clone Repository

```bash
git clone https://github.com/username/nama-repo.git
cd nama-repo
```

### 2. Persiapan Kredensial

Buat file `.env` di root folder:

```env
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DBT_DEV_DB
SNOWFLAKE_SCHEMA=PUBLIC_1_RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

> ⚠️ **Penting:** File `.env` sudah ada di `.gitignore`. Jangan pernah commit file ini ke repository.

### 3. Jalankan Stack dengan Docker

```bash
docker-compose up -d --build
```

Tunggu hingga semua service healthy (sekitar 1-2 menit):

```bash
docker-compose ps   # pastikan semua status "running"
```

### 4. Akses Layanan

| Layanan | URL / Host | Kredensial |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | `airflow` / `airflow` |
| **PostgreSQL** | `localhost:5433` | `airflow` / `airflow` |
| **Database** | `northwind` | — |

### 5. Setup Koneksi di Airflow

Tambahkan koneksi via terminal:

```bash
docker exec -it <container_scheduler> bash

# Koneksi PostgreSQL
airflow connections add postgres_northwind \
  --conn-type postgres \
  --conn-host host.docker.internal \
  --conn-port 5433 \
  --conn-login airflow \
  --conn-password airflow \
  --conn-schema northwind

# Koneksi Snowflake
airflow connections add snowflake_conn \
  --conn-type snowflake \
  --conn-host your_account.snowflakecomputing.com \
  --conn-login your_username \
  --conn-password your_password \
  --conn-schema PUBLIC_1_RAW \
  --conn-extra '{"database": "DBT_DEV_DB", "warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}'
```

### 6. Trigger DAG Ingestion

1. Buka http://localhost:8080
2. Aktifkan DAG `data_northwind_ingestion_postgres_snowflake` (toggle **ON**)
3. Klik **▶ Trigger DAG**
4. Monitor progress di **Graph View**

---

## 📊 Pipeline Ingestion

Pipeline menggunakan metode **Bulk Ingest** via `snowflake.connector.pandas_tools.write_pandas`, memungkinkan pemindahan ribuan baris data secara efisien dalam hitungan detik.

### Tabel yang Di-ingest (9 Tabel)

| Kategori | Tabel |
|---|---|
| **Master Data** | `customer`, `employees`, `suppliers` |
| **Transaksi Penjualan** | `orders`, `order_details` |
| **Produk & Inventori** | `products`, `inventory_transactions` |
| **Pembelian** | `purchase_orders`, `purchase_order_details` |

### Fitur Pipeline

- ✅ **Auto-create table** — tabel otomatis dibuat di Snowflake jika belum ada
- ✅ **Data cleaning** — string kosong `''` dikonversi ke `NULL` sebelum insert
- ✅ **Sequential execution** — tabel diproses satu per satu untuk stabilitas
- ✅ **Error handling** — koneksi selalu di-close dengan `try/finally`
- ✅ **Retry** — task otomatis retry 1x jika gagal

---

## 🧹 Transformasi Data (dbt)

Setelah data mentah mendarat di Snowflake (`PUBLIC_1_RAW`), dbt melakukan transformasi berlapis untuk menghasilkan **Star Schema** siap analitik.

### Layer Transformasi

```
PUBLIC_1_RAW (Raw)
      │
      ▼
  Staging Layer          → Cleaning, rename kolom, casting tipe data
      │
      ▼
  Marts Layer            → Fact & Dimension tables (Star Schema)
```

### Menjalankan dbt

```bash
# Masuk ke container
docker exec -it <container_scheduler> bash
cd /opt/airflow/dbt_project

# Jalankan semua transformasi
dbt run

# Jalankan snapshot (SCD Type 2)
dbt snapshot

# Cek kualitas data
dbt test

# Generate dokumentasi
dbt docs generate && dbt docs serve
```

### Snapshot (SCD Type 2)

Snapshot digunakan untuk **melacak perubahan data historis** (Slowly Changing Dimension Type 2). Setiap perubahan pada data sumber akan direkam dengan kolom `dbt_valid_from` dan `dbt_valid_to`.

---
