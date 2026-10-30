# Stock Market Data Engineering Pipeline

An end-to-end data engineering project demonstrating stock market data ingestion, streaming simulation, cloud storage, and distributed data processing using Apache Kafka, Python, AWS S3, and Databricks.

## Architecture

CSV Historical Data → Kafka Producer → Kafka Topic → Python Consumer → AWS S3 → Databricks → Bronze → Silver → Gold

## Tech Stack

- Python and Pandas
- Apache Kafka
- Docker
- AWS S3
- AWS CLI
- Databricks
- PySpark
- Delta Lake
- SQL

## Project Workflow

1. Historical stock market data is read using Python and Pandas.
2. The Kafka producer publishes stock records to the `stock-prices` topic.
3. A Python Kafka consumer reads messages and stores them in JSONL format.
4. Raw JSONL data is uploaded to an AWS S3 bucket.
5. The data is transferred into Databricks for processing.
6. PySpark processes the data using the Medallion Architecture:
   - Bronze: Raw ingested records.
   - Silver: Cleaned records, duplicate removal and data type conversion.
   - Gold: Aggregated stock market metrics.

## Dataset

The original historical dataset contains 104,224 records across 13 stock market indices, covering 1965–2021.

The current end-to-end Kafka-to-Databricks demonstration uses a smaller sample of records.

## AWS Integration

AWS S3 is used as cloud object storage for raw JSONL data. The initial upload is performed using AWS CLI, and the data is transferred to Databricks using a temporary pre-signed URL.

## Key Learning Outcomes

- Kafka-based data ingestion
- Producer-consumer architecture
- AWS S3 object storage
- PySpark transformations
- Delta Lake tables
- Medallion Architecture
- Data quality validation

## Future Improvements

- Automated S3 ingestion using IAM roles and Unity Catalog external locations.
- Scheduled pipeline execution.
- Larger-scale multi-index data processing.
- Automated data quality checks and monitoring.