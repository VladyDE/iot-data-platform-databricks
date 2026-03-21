# End-to-End IoT Data Engineering Project with Databricks Lakeflow & SDP
## :blue_book: Project Overview
This project implements a complete Lakehouse architecture in order to process IoT sensor telemetry using batch and streaming operations. It features a custom data simulator in Docker that streams Parquet files into Unity Catalog Volumes. The data is processed through a refined Medallion Architecture (Bronze, Silver, Gold) using Spark Declarative Pipelines and orchestrated along with other tasks via Lakeflow Jobs.
## :straight_ruler: Data architecture & Pipeline
<ol>
  <li> <span style="color: blue;">Bronze Layer:</span> Incremental ingestion using Auto Loader. It captures file metadata, ingestion timestamps, and handles initial schema inference.</li>
  <li> Silver Layer: Data cleaning and schema enforcement. I implemented SDP Expectations to controll quality of data by dropping invalid sensor readings before they reach the analytics layer.</li>
  <li> Gold Layer: Fully enriched data from both metadata and telemetry tables. Contains 3 materialized views with aggregations in order to serve the final dashboard.
</li>
</ol>

## :wrench: Tech Stack
<ul>
  <li>Python</li>
  <li>SQL</li>
  <li>Docker</li>
  <li>Spark</li>
  <li>Databricks</li>
  <li>Power BI</li>
</ul>

## :bulb: Challenges and troubleshooting
<ul>
  <li>Cost & Performance Optimization: Used autoloader for data ingestion. This ensures that only new events trigger processing, significantly reducing DBU consumption by limiting full refresh operations.</li>
  <li>Schema Drift Handling: During development, an unexpected schema change in the source files was detected. I resolved this by resetting the schemaLocation and enforcing a strict schema policy to prevent data corruption.</li>
  <li>Idempotency & Cleanup: Developed a maintenance script using Task Values to identify the latest metadata file and perform a safe cleanup of obsolete files in the Volume before the pipeline execution.</li>
</ul>
