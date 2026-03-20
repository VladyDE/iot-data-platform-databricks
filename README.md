# End-to-End Data Engineering Project with Databricks Lakeflow & SDP
## :blue_book: Project Overview
This project implements a complete Lakehouse architecture in order to process IoT sensor telemetry using batch and streaming operations. It features a custom data simulator in Docker that streams Parquet files into Unity Catalog Volumes. The data is processed through a refined Medallion Architecture (Bronze, Silver, Gold) using Spark Declarative Pipelines and orchestrated along with other tasks via Lakeflow Jobs.
## :straight_ruler: Data architecture & Pipeline
<ol>
  <li>Bronze Layer (Raw): Incremental ingestion using Auto Loader (cloudFiles). It captures file metadata, ingestion timestamps, and handles initial schema inference.
</li>
  <li>Silver Layer (Curated): Data cleaning and schema enforcement. I implemented DLT Expectations to act as a "Data Quality Gate," dropping invalid sensor readings (e.g., out-of-range temperature or humidity) before they reach the analytics layer.</li>
  <li>Gold Layer (Analytics): * full_iot_info_gold: A Streaming Table performing an incremental Join with static sensor metadata for low-latency enrichment.
</li>
</ol>
## Tech Stack
<ul>
  <li>Orchestration: Databricks Lakeflow Jobs (utilizing Task Values for dynamic variable passing between tasks).</li>
  <li>Processing Engine: Spark Declarative Pipelines with Python & SQL.</li>
  <li>Ingestion: Auto Loader with Schema Evolution protection</li>
  <li>Governance: Unity Catalog</li>
  <li>Governance: Unity Catalog</li>
  <li>Visualization: Power BI via DirectQuery for real-time insights.</li>
  <li>Infrastructure: Docker.</li>
</ul>
