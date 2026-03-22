# End-to-End IoT Data Engineering Project with Databricks Lakeflow & SDP

## :blue_book: Project Overview
This project implements a complete Lakehouse architecture in order to process IoT sensor telemetry using batch and streaming operations. It features a custom data simulator in Docker that streams Parquet files into Unity Catalog Volumes. The data is processed through a refined Medallion Architecture (Bronze, Silver, Gold) using Spark Declarative Pipelines and orchestrated along with other tasks via Lakeflow Jobs.
The whole pipeline is idempotent, thanks to the SDP task and operations like `MERGE INTO` in other tasks withing the lakeflow job.

## :straight_ruler: Data architecture & Pipeline
1. Bronze Layer: Incremental ingestion using Auto Loader. It captures file metadata, ingestion timestamps, and handles initial schema inference.
2. Silver Layer: Data cleaning and schema enforcement. I implemented SDP Expectations to controll quality of data by dropping invalid sensor readings and flagin those with unexpected values with warnings before they reach the analytics layer.
3. Gold Layer: Fully enriched data from both sensor metadata and telemetry tables. Contains 3 materialized views with aggregations in order to serve the final dashboard.

![Alt text](iot-data-platform-databricks/img/Pipeline_Graph.png)

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

## :shield: Edge Cases & System Resilience
### 1. Late-Arriving Dimensions (sensor metadata)
A new IoT sensor is deployed and begins transmitting telemetry before its metadata has been registered in the `dim_sensors` table.
In this case a standard JOIN will result in droping records in the background without us noticing that they are gone. So in order to solve this i implementend a `LEFT JOIN` along with `COALESCE` function to flag those records as "pending registration" so we not end up with NULLs in the dashboard.

### 2. Schema Drift & Malformed Payload Handling
An upstream change in the ingestion or a corrupted Parquet file introduces unexpected columns or incompatible data types.
The schema mismatch can easily crash the entire Spark structured streaming job (autoloader) or in the worse case scenario pollute the Silver/Gold layers with "garbage" data. For this case we use `cloudFiles.schemaEvolutionMode: failFast` in order to handle schema mismatch between the parquet that's being ingested and the actual schema of the delta tables, also the silver layer has expectations that act as another layer of security to prevent corrupted data to advance troughout the medallion architecture.
