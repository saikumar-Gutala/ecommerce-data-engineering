from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import sys

PROJECT_HOME = "/home/hdpuser/ecommerce-data-engineering"

sys.path.insert(0, PROJECT_HOME + "/src")
from monitoring_logger import log_event

ENV_SETUP = (
    "export HADOOP_HOME=/home/hdpuser/hadoop-3.2.0; "
    "export SPARK_HOME=/home/hdpuser/spark-3.2.1-bin-hadoop3.2; "
    "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk; "
    "export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$PATH; "
)

def task_success(context):
    task_id = context["task_instance"].task_id
    log_event(task_id, "SUCCESS", "Generated pipeline task completed successfully")

def task_failure(context):
    task_id = context["task_instance"].task_id
    error = str(context.get("exception", "Unknown error"))
    log_event(task_id, "FAILED", error)

default_args = {
    "owner": "hdpuser",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_success_callback": task_success,
    "on_failure_callback": task_failure,
}

with DAG(
    dag_id="ecommerce_generated_pipeline",
    default_args=default_args,
    description="Generated E-Commerce pipeline with Spark, Delta and monitoring",
    start_date=datetime(2026, 11, 12),
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "generated", "spark", "delta", "monitoring"],
) as dag:

    check_input = BashOperator(
        task_id="check_generated_hdfs_input",
        bash_command=(
            ENV_SETUP +
            "hdfs dfs -ls /ecommerce/generated/raw/"
        ),
    )

    data_quality = BashOperator(
        task_id="validate_generated_data",
        bash_command=(
            "cd " + PROJECT_HOME +
            " && DATA_DIR=" + PROJECT_HOME + "/data/generated "
            "REPORT_FILE=" + PROJECT_HOME + "/logs/generated_data_quality_report.txt "
            "python3 src/data_quality.py"
        ),
    )

    spark_batch = BashOperator(
        task_id="run_generated_spark_batch",
        bash_command=(
            ENV_SETUP +
            "cd " + PROJECT_HOME +
            " && spark-submit --master 'local[2]' "
            "--conf spark.sql.shuffle.partitions=4 "
            "src/spark/ecommerce_generated_batch.py"
        ),
    )

    delta_merge = BashOperator(
        task_id="merge_generated_delta",
        bash_command=(
            ENV_SETUP +
            "cd " + PROJECT_HOME +
            " && spark-submit --master 'local[2]' "
            "--packages io.delta:delta-core_2.12:2.0.2 "
            "--conf spark.sql.shuffle.partitions=4 "
            "src/spark/generated_delta_merge.py"
        ),
    )

    verify_delta = BashOperator(
        task_id="verify_generated_delta",
        bash_command=(
            ENV_SETUP +
            "hdfs dfs -ls /ecommerce/generated/delta/order_summary/_delta_log/"
        ),
    )

    check_input >> data_quality >> spark_batch >> delta_merge >> verify_delta
