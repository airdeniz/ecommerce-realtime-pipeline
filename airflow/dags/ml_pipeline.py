from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ML_DIR = "/opt/airflow/ml"


def spark_submit(script: str) -> str:
    """Build a spark-submit command for one ML job.

    The Iceberg/MinIO/JDBC JARs are baked into the image at /opt/ml-jars; they are
    collected into a comma-separated --jars list at runtime. Each job runs a local
    Spark engine (local[*]) inside the scheduler — no separate Spark cluster.
    """
    return (
        'JARS=$(ls /opt/ml-jars/*.jar | tr "\\n" "," | sed "s/,$//"); '
        'spark-submit --master "local[*]" --driver-memory 2g '
        f'--jars "$JARS" --py-files {ML_DIR}/common.py {ML_DIR}/{script}'
    )


with DAG(
    dag_id="ml_pipeline",
    default_args=default_args,
    description="Train ML models on the lakehouse and write results to lakehouse.ml",
    # Runs after the dbt pipeline (02:00) has refreshed silver/gold and the
    # lakehouse.ml_features feature tables the ML jobs read from.
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "lakehouse"],
) as dag:

    fraud = BashOperator(
        task_id="fraud_anomaly",
        bash_command=spark_submit("fraud_isolation_forest.py"),
    )

    forecast = BashOperator(
        task_id="demand_forecast",
        bash_command=spark_submit("demand_forecast.py"),
    )

    segmentation = BashOperator(
        task_id="customer_segmentation",
        bash_command=spark_submit("customer_segmentation.py"),
    )

    churn = BashOperator(
        task_id="churn_prediction",
        bash_command=spark_submit("churn_prediction.py"),
    )

    # Chained sequentially: each job spins up its own local Spark JVM, so running
    # them one at a time avoids memory contention on a single machine.
    fraud >> forecast >> segmentation >> churn
