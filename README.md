# E-Commerce Data Engineering Pipeline

An end-to-end Big Data Engineering project using Hadoop, Kafka, Spark, Delta Lake, Python and Apache Airflow.

## Technology Stack

- Apache Hadoop 3.2.0 – Distributed storage (HDFS)
- Apache Kafka 3.7.2 – Event streaming
- Apache Spark 3.2.1 – Batch and stream processing
- Delta Lake 2.0.2 – ACID transactions and MERGE
- Apache Airflow 2.2.2 – Workflow orchestration
- Python – Data generation and validation
- Linux – Execution environment

## Architecture

Batch:
CSV Data → HDFS → Data Quality → Spark → Delta Lake

Streaming:
Kafka Producer → Kafka Topics → Spark Structured Streaming → HDFS / Delta Lake

Orchestration:
Apache Airflow → Pipeline Tasks → Monitoring Logs

## Main Modules

1. Data generation – Customers, products, orders and order items.
2. Hadoop – HDFS storage and data management.
3. Kafka – Producers, consumers and event ingestion.
4. Spark – DataFrames, SQL, joins, aggregations and streaming.
5. Delta Lake – Tables, MERGE, deduplication and history.
6. Data Quality – Validation, duplicate detection and quarantine.
7. Airflow – Automated workflow orchestration.
8. Monitoring – Execution logs and failure tracking.
9. Spark Optimization – Partitioning, caching, broadcast joins and data skew handling.

## Airflow Pipelines

### ecommerce_data_pipeline
1. Check HDFS input
2. Data quality check
3. Run Spark batch
4. Merge into Delta
5. Verify Delta table

### ecommerce_generated_pipeline
1. Check generated HDFS input
2. Validate generated data
3. Run generated Spark batch
4. Merge generated Delta
5. Verify generated Delta

Both DAGs have retry configuration and success/failure monitoring callbacks.

## Important Directories

- `data/raw/` – Raw datasets
- `data/generated/` – Generated datasets
- `dags/` – Airflow DAGs
- `src/` – Python source code
- `src/spark/` – Spark scripts
- `src/producers/` – Kafka producers
- `logs/` – Pipeline logs and reports
- `docs/` – Project documentation

## HDFS Locations

- `/ecommerce/raw/`
- `/ecommerce/generated/raw/`
- `/ecommerce/generated/processed/`
- `/ecommerce/delta/`
- `/ecommerce/generated/delta/`
- `/ecommerce/streaming/delta/`

## Validation

The generated batch output was validated in HDFS:
`/ecommerce/generated/processed/order_summary_final_validation`

It contained 3,760 data records plus one CSV header line.

Historical successful DAG runs are available. Current Airflow startup is blocked by the system SQLite library version, older than the minimum required by Airflow 2.2.2.

## Learning Outcomes

- Building batch and streaming data pipelines
- Distributed storage with HDFS
- Data processing with Apache Spark
- Data validation and deduplication
- Delta Lake transactional operations
- Workflow orchestration with Airflow
- Pipeline monitoring and Spark optimization

## Setup and Execution

### Environment

export PROJECT_HOME=/home/hdpuser/ecommerce-data-engineering
export HADOOP_HOME=/home/hdpuser/hadoop-3.2.0
export SPARK_HOME=/home/hdpuser/spark-3.2.1-bin-hadoop3.2
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

### Batch Execution

Original pipeline:
- python3 src/data_quality.py
- spark-submit src/spark/ecommerce_batch.py
- spark-submit src/spark/delta_merge_from_airflow.py

Generated pipeline:
- python3 src/data_quality.py
- spark-submit src/spark/ecommerce_generated_batch.py
- spark-submit src/spark/generated_delta_merge.py

### Airflow

DAGs: ecommerce_data_pipeline and ecommerce_generated_pipeline.
Both DAGs have five sequential tasks, retry configuration and monitoring callbacks.

### Streaming

Kafka Producer -> Kafka -> Spark Structured Streaming -> Delta Lake.
HDFS checkpoints are maintained under /ecommerce/streaming/checkpoints/.

### Runtime Notes

Airflow 2.2.2 is installed in the project virtual environment, but startup currently fails because the system SQLite C library is older than 3.15.0.
A separate clean Kafka KRaft data directory was used for live integration testing. Original Kafka data and backups were preserved. The live Kafka-to-Spark-to-Delta data quality flow was successfully verified.
Previously persisted HDFS outputs and successful historical execution logs are available.

## Project Highlights

### Data Quality Engineering
- Validates mandatory order, customer and product identifiers.
- Detects null and invalid quantity and amount values.
- Validates order status against allowed values.
- Separates invalid records into a quarantine Delta table.
- Generates data quality metrics and validation reports.

### Streaming Pipeline
- Ingests order events through Kafka.
- Processes events using Spark Structured Streaming.
- Uses checkpoints for streaming recovery.
- Writes valid and invalid records to separate Delta outputs.
- Implements deduplication and validation.

### Delta Lake
- Creates transactional Delta tables.
- Performs MERGE operations for incremental data.
- Maintains Delta transaction history.
- Supports validated batch and streaming outputs.

### Spark Performance Experiments
- Adaptive Query Execution (AQE).
- Broadcast joins.
- Cache versus persist.
- Repartition versus coalesce.
- Shuffle partition tuning.
- Data skew handling and salting.

## Verified Results

- Generated batch validation: 3,760 data records plus CSV header.
- Live Kafka-to-Spark-to-Delta integration successfully verified.
- A live test order was verified in the valid Delta output.
- Five invalid records were verified in the quarantine output.
- HDFS cluster verification showed four live DataNodes.
- No corrupt or missing HDFS blocks were reported during verification.

## Known Limitations and Handover Status

- Kafka broker is currently stopped due to a KRaft metadata consistency error during startup. Existing Kafka storage was backed up to `/home/hdpuser/kafka-backup-20261120/kraft-combined-logs` and verified against the original.
- Kafka streaming integration was successfully tested earlier, including valid and quarantined records. Current live streaming availability is pending Kafka recovery.
- Airflow 2.2.2 runtime and SQLite compatibility issue has been resolved. Both project DAGs import successfully with no import errors.
- Airflow DAG execution was intentionally skipped; no DAG runs are recorded.
- Kafka test topic used replication factor 1; this setup does not provide broker-level replica redundancy.
- Spark UI browser inspection remains pending.
- Hadoop verification: four live DataNodes, zero missing blocks, zero corrupt blocks, and zero under-replicated blocks.

## Author

**Saikumar Gutala**

GitHub: https://github.com/saikumar-Gutala

Project Repository:
https://github.com/saikumar-Gutala/ecommerce-data-engineering
