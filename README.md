# End-to-End IoT Data Engineering Project with Databricks Lakeflow & SDP

## :blue_book: Project Overview
This project implements a complete Lakehouse architecture in order to process IoT sensor telemetry using batch and streaming operations. It features a custom data simulator in Docker that streams Parquet files into Unity Catalog Volumes. The data is processed through a refined Medallion Architecture (Bronze, Silver, Gold) using Spark Declarative Pipelines and orchestrated along with other tasks via Lakeflow Jobs.

## :straight_ruler: Data architecture & Pipeline
1. Bronze Layer: Incremental ingestion using Auto Loader. It captures file metadata, ingestion timestamps, and handles initial schema inference.
2. Silver Layer: Data cleaning and schema enforcement. I implemented SDP Expectations to controll quality of data by dropping invalid sensor readings before they reach the analytics layer.
3. Gold Layer: Fully enriched data from both metadata and telemetry tables. Contains 3 materialized views with aggregations in order to serve the final dashboard.

## :wrench: Tech Stack
- Python
- SQL
- Docker
- Spark
- Databricks
- Power BI

## :bulb: Challenges and troubleshooting
- Cost & Performance Optimization: Used autoloader for data ingestion. This ensures that only new events trigger processing, significantly reducing DBU consumption by limiting full refresh operations
- Schema Drift Handling: During development, an unexpected schema change in the source files was detected. I resolved this by resetting the schemaLocation and enforcing a strict schema policy to prevent data corruption.
- Idempotency & Cleanup: Developed a maintenance script using Task Values to identify the latest metadata file and perform a safe cleanup of obsolete files in the Volume before the pipeline execution.

## :rocket: Deployment Steps

- Build and run the docker image in `/simulation`
- Create a new SDP pipeline and configure the `SDP_pipelines/IoTProject_ETL` as the path.
- Import the `job_config.yml` into Databricks Lakeflow Jobs (make sure to edit the path to each volume and provide the pipeline id)

