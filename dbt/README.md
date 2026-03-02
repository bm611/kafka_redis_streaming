# Booking Ads Analytics Pipeline (Airflow + dbt)

An ELT pipeline that transforms raw booking and ad bidding data into analytics-ready reporting tables, orchestrated by Airflow and modeled with dbt.

## Use Case

This project simulates the **Booking Ads** team's data foundation at Booking.com. Third-party ad providers send bidding data (impressions, clicks, costs), while the platform captures booking events. The pipeline joins these sources to produce reporting marts that answer:

- **What is the click-through rate (CTR) and return on ad spend (ROAS) per partner?**
- **Which ad placements drive the most bookings?**
- **How does revenue attribution break down by country and hotel?**
- **Which campaigns should we scale or cut?**

This mirrors the JD's goal: *"evolving the data foundation to improve reporting based on ads bidding data from third-party providers"* and *"provide reusable data assets to enhance machine learning models."*

## Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────────────────────────┐
│  seed_raw_   │     │             │     │           dbt models                 │
│  data.py     │────▶│  Postgres   │────▶│  staging → intermediate → marts     │
│  (Faker)     │     │  (warehouse)│     │  + data quality tests               │
└──────────────┘     └─────────────┘     └──────────────────────────────────────┘
                           ▲                              │
                           │                              │
                     ┌─────┴──────┐                       │
                     │  Airflow   │◀──────────────────────┘
                     │  (DAG)     │   orchestrates dbt run + test
                     └────────────┘
```

### Data Flow

| Layer | Tables | Purpose |
|---|---|---|
| **Raw** | `raw_bookings`, `raw_ad_impressions`, `raw_ad_clicks` | Simulated source data (Faker) |
| **Staging** | `stg_bookings`, `stg_ad_impressions`, `stg_ad_clicks` | Cleaned, typed, deduplicated |
| **Intermediate** | `int_booking_ads_joined` | Bookings enriched with ad attribution |
| **Marts** | `fct_ad_performance`, `dim_hotels`, `dim_partners` | Star schema for reporting & ML |

### Data Model (Star Schema)

```
              ┌──────────────┐
              │ dim_partners │
              │──────────────│
              │ partner_id   │
              │ partner_name │
              │ channel      │
              └──────┬───────┘
                     │
┌──────────────┐     │     ┌────────────────────┐
│  dim_hotels  │     │     │ fct_ad_performance │
│──────────────│     │     │────────────────────│
│ hotel_id     │─────┼────▶│ impressions        │
│ hotel_name   │     │     │ clicks             │
│ country      │     │     │ ctr                │
└──────────────┘     │     │ spend              │
                     │     │ bookings           │
                     └────▶│ revenue            │
                           │ roas               │
                           └────────────────────┘
```

## Tech Stack

| Tool | Purpose | Relevance to JD |
|---|---|---|
| **Postgres** | Data warehouse | Simulates Snowflake locally |
| **dbt** | Data modeling & quality tests | "dbt or equivalent tools" |
| **Airflow** | Pipeline orchestration | "Airflow" |
| **Python (Faker)** | Simulates raw data | "Proven knowledge of Python" |
| **Docker Compose** | Local infrastructure | Same pattern as Kafka project |

## Quick Start

### 1. Start Infrastructure

```bash
cd docker
docker compose up -d
cd ..
```

This starts **Postgres** (warehouse) and **Airflow** (`standalone` mode: api-server + scheduler + dag-processor in a single container) with dbt pre-installed.

The admin password is generated on first boot — check the logs:

```bash
docker logs booking_airflow 2>&1 | grep "Password for user"
```

### 2. Seed Raw Data

Generate fake booking and ad data into Postgres:

```bash
pip install psycopg2-binary faker
# or with uv:
uv sync

python scripts/seed_raw_data.py
```

This creates and populates `raw_bookings`, `raw_ad_impressions`, and `raw_ad_clicks` with ~10k rows each.

### 3. Run dbt Manually (Optional)

You can run dbt directly to test the models before involving Airflow:

```bash
cd booking_analytics
dbt debug          # verify connection
dbt run            # build all models
dbt test           # run data quality tests
cd ..
```

### 4. Trigger via Airflow

Open the Airflow UI at [http://localhost:8080](http://localhost:8080) and log in with the credentials from step 1.

Enable and trigger the `booking_ads_dbt_pipeline` DAG. It will:
1. `dbt debug` — verify connection
2. `dbt deps` — install packages (e.g. `dbt_utils`)
3. `dbt run --select staging` → `dbt test --select staging`
4. `dbt run --select intermediate marts` → `dbt test --select marts`

### 5. Query the Marts

```bash
docker exec -it booking_warehouse psql -U booking_user -d booking_analytics

-- Ad performance by partner
SELECT * FROM marts.fct_ad_performance ORDER BY roas DESC;

-- Revenue by hotel
SELECT * FROM marts.dim_hotels;
```

## Key Concepts

| Concept | Implementation |
|---|---|
| **ELT Pattern** | Raw data loaded first, transformations happen in-warehouse via dbt |
| **Star Schema** | Fact table (`fct_ad_performance`) + dimension tables (`dim_hotels`, `dim_partners`) |
| **Data Quality** | dbt `data_tests`: `not_null`, `unique`, `accepted_values`, `relationships`, `dbt_utils.accepted_range` |
| **Orchestration** | Airflow DAG with staged quality gates: debug → deps → staging (run/test) → marts (run/test) |
| **Reusable Assets** | Staging models are reusable across multiple marts (for ML, reporting, etc.) |
| **Schema Design** | Raw → staging → intermediate → marts layering pattern |

## Data Quality Tests

| Test | Table | Column | Purpose |
|---|---|---|---|
| `not_null` | `stg_bookings` | `booking_id` | Every booking must have an ID |
| `unique` | `stg_bookings` | `booking_id` | No duplicate bookings |
| `accepted_values` | `stg_bookings` | `payment_method` | Only known payment methods |
| `relationships` | `fct_ad_performance` | `hotel_id` | FK integrity to `dim_hotels` |
| `dbt_utils.accepted_range` | `fct_ad_performance` | `revenue` | Revenue must be ≥ 0 |
| `dbt_utils.accepted_range` | `fct_ad_performance` | `ctr` | CTR must be between 0 and 1 |
| `dbt_utils.accepted_range` | `fct_ad_performance` | `roas` | ROAS must be ≥ 0 |

## Project Structure

```
dbt/
├── docker/
│   ├── docker-compose.yml       # Postgres + Airflow
│   └── Dockerfile.airflow       # Airflow image with dbt installed
├── dags/
│   └── booking_ads_pipeline.py  # Airflow DAG
├── booking_analytics/           # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── scripts/
│   ├── init_raw_tables.sql      # DDL for raw tables
│   └── seed_raw_data.py         # Faker-based data generator
└── README.md
```
