# E-commerce Data Pipeline

## Table of Contents

- [1) Introduction](#1-introduction)
- [2) Source Data](#2-source-data)
- [3) Architecture](#3-architecture)
- [4) Code Structure](#4-code-structure)
- [5) Data Anomalies Disclaimer](#5-data-anomalies-disclaimer)
- [6) Tech Stack](#6-tech-stack)
- [7) Start Guide](#7-start-guide)

## 1) Introduction

This project implements an end-to-end data engineering pipeline for an e-commerce
platform's sales and payments data. It ingests 8 raw CSV files from an OLTP export, moves them through a data lake, transforms them
into a dimensional model in a cloud data warehouse, and surfaces the results in a
business intelligence dashboard, all orchestrated, tested, and monitored end to end.

The goal was to build this the way a real data team would: a proper Bronze → Silver →
Gold layering, incremental loading where it matters, automated data quality tests,
and Slack alerting on pipeline success/failure instead of creating just a one-off script that moves
data from A to B.

## 2) Source Data

The source is a dataset about E-commerce with 8 tables, modeled as follows:
![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/data-model.png)

After Transformation, this is my data model:
![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/data-model-1.png)


## 3) Architecture 

![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/diagram-1.png)

![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/diagram-2.png)

In my architecture, Dagster first check for input environment to verify user put enough credentials as input, followed by checking the location of raw data before processing. Then, it will read the 8 CSVs and upload to Amazon S3 Server. On success, it will send a slack message as notification. The surrogate key is the an artificial row id that is created to record changes on some entity in a table (for example order_id). In this code, every column will contribute directly into hashing surrogate key to ensure uniqueness. These are my transformation specifically.

- **dim_customers**: This was splitted into dim_customer that contains customer_id (customer_unique_id from raw.customers) and customer_address_id (customer_id from raw.customers) and dim_customer_address that contains customer_address_id (customer_id from raw.customers), customer_zip_code_prefix, customer_city and customer_state. Both are added with Surrogate key.
- **dim_orders**: Add Surrogate Key. Set materialized type to Incremental.
- **dim_products**: Merge with raw_product_category_name_translation. Some special categories are "raw_product_category_name_translation" (meaning "portable kitchen appliances and food preparers") and pc_gamer (keep the same) that did not appeared in English Translation. These are exceptionally handled.
- **dim_seller**: Create Surrogate Key.
- **fact_order_items**: For easier Power BI calculation, I took customer_id from raw_orders and added to this table. Create Surrogate Key and set materialized type to Incremental.
- **fact_order_payment**: Add Surrogate Key and set materialized type to Incremental.
- **fact_order_reviews**: Take these columns only review_id, order_id, review_score, review_creation_date, review_answer_timestamp. Add Surrogate Key and set materialized type to Incremental.
- Applied dbt.utils to Generate Surrogate Key and Check for combination of columns for duplicates.


  
- Below is the row counts for each table:
![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/result.jpg)

## 4) Code Structure

```
.
├── docker-compose.yaml
├── .env (Users are required to create this manually on their local machine to add credentials)
├── docker/
│   ├── Dockerfile              # single image for all 3 Dagster services.
│   ├── dagster.yaml            # Dagster instance config (Postgres storage).
│   ├── workspace.yaml          # points webserver/daemon at the code server.
│   └── requirements.txt        # package requirements.
├── data/
│   └── raw/                    # drop the 8 source CSVs here.
├── spark_jobs/
│   └── load_to_snowflake.py    # S3 -> Snowflake RAW, run via spark-submit.
├── ecom_dagster/
│   └── ecom_dagster/
│       ├── definitions.py      # wires assets, resources, jobs, sensors together.
│       ├── assets.py           # the full asset graph (env check -> ... -> dbt build -> notify).
│       └── tasks.py            # plain business logic, no Dagster imports (unit-testable).
└── ecom_dbt/
    ├── dbt_project.yml
    ├── profiles.yml            # Snowflake connection, reads from env vars.
    ├── macros/
    │   └── not_mostly_null.sql # custom data quality test.
    └── models/
        ├── raw/                # ephemeral - 1:1 source() reads, never persisted
        ├── dim/                # dimension tables: customers, product_category_name_translation, seller, etc along with schema.
        └── fact/               # fact tables: order_items, order_payments, order_reviews, etc along with schema
```


## 5) Data Anomalies Disclaimer

These are some data Anomalies that has been found inside the source:
![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/anomaly-1.png)

In Order Reviews, some order_id has multiple reviews, followed by some updates in review score. I will keep these rows to ensure objectivity in my visualization.

![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/anomaly-2.png)

In some Orders, the total payment values are different from total price of all items. This could be caused by some other factors such as shipping cost, tax, extra services, etc. Similarly, I will keep these values to ensure objectivity in my visualization.

## 6) Tech Stack

**Dagster** — The orchestrator tying every stage together into one dependency graph, from environment validation through S3 ingestion, the Spark load, and the dbt build. Instead of running scripts manually in sequence, Dagster lets the whole pipeline execute (be monitored, retried, and alerted on) as a single "Materialize all" click, which is what makes this a *pipeline* rather than a collection of scripts.

**Amazon S3** — The data lake layer, holding a raw snapshot of the source CSVs before anything touches the warehouse. This decouples ingestion from transformation: if a downstream step ever needs to be re-run or debugged, the original data is sitting there untouched, rather than only existing as a fleeting extract from the source system.

**Apache Spark** — Handles the actual lake-to-warehouse load, reading CSVs from S3 and writing them into Snowflake at scale. Using a real distributed engine here (rather than a single-threaded script) means this step is built to handle data volumes far beyond this project's dataset without a rewrite.

**Snowflake** — The cloud data warehouse where all transformation and modeling happens. Its separation of storage and compute, combined with native support for schemas like `RAW` and `dev_michael`, made it possible to cleanly separate the untouched bronze data from the transformed dimensional model without needing separate infrastructure.

**dbt** — Turns raw warehouse tables into a tested, documented dimensional model using pure SQL. This is where data quality actually gets enforced — uniqueness/null tests, a custom null-threshold test, and incremental logic — so that "the pipeline ran successfully" and "the data is actually correct" are two separately verified things, not the same assumption.

**Power BI** — The final layer, turning the dimensional model into an interactive dashboard for business stakeholders. This is the payoff step: everything upstream exists so that someone with no SQL knowledge can explore sales trends and get answers without needing to touch the warehouse directly. (Will be added in another Github Repository)

**Slack** — Closes the loop on observability. Rather than needing to check Dagit to know whether a run succeeded, ingestion summaries and transform-complete notifications land directly in a channel — which matters most exactly when you're *not* watching the pipeline run.


## 7) Start guide
### a) Prerequisites
- Docker + Docker Compose.
- An AWS S3 bucket with read/write credentials.
- A Snowflake account with a warehouse and database you can create schemas in, followed by creating an user that dbt can access in and user that Power BI can extract the transformed dataset (**REMEBER TO DISABLE MFA AUTHENTICATION BEFOER USING**).
- (Optional) A Slack incoming webhook for pipeline notifications.
### b) Set up: 

- Create and fill in your .env credentials:
  
```bash

AWS_ACCESS_KEY_ID=<your_aws_access_key>
AWS_SECRET_ACCESS_KEY=<your_aws_secret_access_key>
AWS_REGION=ap-southeast-4
AWS_S3_BUCKET=ecom-michael
SNOWFLAKE_ACCOUNT=<your_snowflake_account>
SNOWFLAKE_USER=<dbt_user>
SNOWFLAKE_PASSWORD=<dbt_password
SNOWFLAKE_ROLE=TRANSFORM
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ecom_michael
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_SCHEMA1=DEV_MICHAEL
SLACK_WEBHOOK_URL= <Slack Webhook URL- can be obtained by creating from Slack API)
SLACK_CHANNEL=#ecom-pipeline
DBT_TARGET=dev
DAGSTER_POSTGRES_USER=postgres
DAGSTER_POSTGRES_PASSWORD=postgres
DAGSTER_POSTGRES_DB=postgres
```

- Initiate the container:
```bash
docker compose up -d --build
```

- Accees http://localhost:3000 and go to Jobs => ecom_pipeline => Materialize all and wait for the process to be done.
- You will see a Slack notification similar to this:
![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/result-1.png)

![alt text](https://github.com/minhD03/ecom-project/blob/f441ad03672fd8b617aa6493023feec45a10359a/images/result-4.png)

- Go to Power BI and import the data into there.
