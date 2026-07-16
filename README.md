# E-commerce Data Pipeline

## Table of Contents

- [1) Introduction](#1-introduction)
- [2) Source Data](#2-source-data)
- [3) Architecture](#3-architecture)
- [4) Code Structure](#4-code-structure)
- [5) Tech Stack](#5-tech-stack)
- [6) Start Guide](#6-start-guide)

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
![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/data-model.png)


## 3) Architecture 

![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/diagram-1.png)

![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/diagram-2.png)

In my architecture, Dagster first check for input environment to verify user put enough credentials as input, followed by checking the location of raw data before processing. Then, it will read the 8 CSVs and upload to Amazon S3 Server. On success, it will send a slack message as notification.

- **dim_customers, dim_product_name_category_name_translate, dim_seller**: No changes were made.
- **fact_order_items, fact_order_payments, fact_orders**: Set the data as incremental to just updates the new entries every following runs.
- **fact_order_reviews**: select these columns only: review_id, order_i, review_score, review_creation_date, review_answer_timestamp.
- **fact_products**: rename the column names from "length" to "length", only select the columns where the product_category_name is not null.
- Most of the tables were tested with not null and unique conditions. Howver, for fact_orders and fact_products, i tested with a row where more than half of the columns are null. These datasets were contributed meaningfully as long as the rest of information (except unique_id) were not null.
- Below is the row counts for each table:
![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/result.jpg)

## 4) Code Structure:

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
    ├── profiles.yml            # Snowflake connection, reads from env vars
    ├── macros/
    │   └── not_mostly_null.sql # custom data quality test
    └── models/
        ├── raw/                # ephemeral - 1:1 source() reads, never persisted
        ├── dim/                # dimension tables: customers, product_category_name_translation, seller along with schema.
        └── fact/               # fact tables: order_items, order_payments, order_reviews, orders, products along with schema
```
## 5) Tech Stack:

**Dagster** — The orchestrator tying every stage together into one dependency graph, from environment validation through S3 ingestion, the Spark load, and the dbt build. Instead of running scripts manually in sequence, Dagster lets the whole pipeline execute (be monitored, retried, and alerted on) as a single "Materialize all" click, which is what makes this a *pipeline* rather than a collection of scripts.

**Amazon S3** — The data lake layer, holding a raw snapshot of the source CSVs before anything touches the warehouse. This decouples ingestion from transformation: if a downstream step ever needs to be re-run or debugged, the original data is sitting there untouched, rather than only existing as a fleeting extract from the source system.

**Apache Spark** — Handles the actual lake-to-warehouse load, reading CSVs from S3 and writing them into Snowflake at scale. Using a real distributed engine here (rather than a single-threaded script) means this step is built to handle data volumes far beyond this project's dataset without a rewrite.

**Snowflake** — The cloud data warehouse where all transformation and modeling happens. Its separation of storage and compute, combined with native support for schemas like `RAW` and `dev_michael`, made it possible to cleanly separate the untouched bronze data from the transformed dimensional model without needing separate infrastructure.

**dbt** — Turns raw warehouse tables into a tested, documented dimensional model using pure SQL. This is where data quality actually gets enforced — uniqueness/null tests, a custom null-threshold test, and incremental logic — so that "the pipeline ran successfully" and "the data is actually correct" are two separately verified things, not the same assumption.

**Power BI** — The final layer, turning the dimensional model into an interactive dashboard for business stakeholders. This is the payoff step: everything upstream exists so that someone with no SQL knowledge can explore sales trends and get answers without needing to touch the warehouse directly. (Will be added in another Github Repository)

**Slack** — Closes the loop on observability. Rather than needing to check Dagit to know whether a run succeeded, ingestion summaries and transform-complete notifications land directly in a channel — which matters most exactly when you're *not* watching the pipeline run.


## 6) Start guide:
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
![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/result-1.png)

![alt text](https://github.com/minhD03/ecom-project/blob/806df293f9913d8e2ab5051fd2079950c1ea490f/images/result-4.png)

- Go to Power BI and import the data into there.
