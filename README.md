# End-to-End Data Engineering Project with Databricks Lakeflow & SDP
## Project Overview
This project implements a complete Lakehouse architecture in order to process IoT sensor telemetry using batch and streaming operations. It features a custom data simulator in Docker that streams Parquet files into Unity Catalog Volumes. The data is processed through a refined Medallion Architecture (Bronze, Silver, Gold) using Spark Declarative Pipelines and orchestrated along with other tasks via Lakeflow Jobs.
## Data architecture & Pipeline
-> Bronze Layer (Raw): Incremental ingestion using Auto Loader (cloudFiles). It captures file metadata, ingestion timestamps, and handles initial schema inference.

-> Silver Layer (Curated): Data cleaning and schema enforcement. I implemented DLT Expectations to act as a "Data Quality Gate," dropping invalid sensor readings (e.g., out-of-range temperature or humidity) before they reach the analytics layer.

-> Gold Layer (Analytics): * full_iot_info_gold: A Streaming Table performing an incremental Join with static sensor metadata for low-latency enrichment.
