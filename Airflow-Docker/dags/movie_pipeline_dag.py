from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="movie_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["movie", "data_engineering"]
) as dag:

    # -------------------------------
    # 1. BRONZE LAYER (Ingestion)
    # -------------------------------
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /opt/airflow/project/pipelines/bronze_layer/upload_stage.py"
    )

    # -------------------------------
    # 2. SILVER LAYER (Cleaning)
    # -------------------------------
    run_silver_transformations = BashOperator(
        task_id="run_silver_transformations",
        bash_command="python /opt/airflow/project/pipelines/silver_layer/silver_transformations.py"
    )

    # -------------------------------
    # 3. VERIFY SILVER TABLES
    # -------------------------------
    verify_silver_tables = SnowflakeOperator(
        task_id="verify_silver_tables",
        snowflake_conn_id="snowflake_default",
        sql="""
        SELECT COUNT(*) FROM SILVER.tmdb_movies_clean;
        SELECT COUNT(*) FROM SILVER.boxoffice_clean;
        SELECT COUNT(*) FROM SILVER.imdb_ratings_clean;
        SELECT COUNT(*) FROM SILVER.imdb_titles_clean;
        SELECT COUNT(*) FROM SILVER.ott_platforms_clean;
        """
    )

    # -------------------------------
    # 4. GOLD LAYER (Analytics)
    # -------------------------------
    run_gold_transformations = BashOperator(
        task_id="run_gold_transformations",
        bash_command="python /opt/airflow/project/pipelines/gold_layer/gold_transformations.py"
    )

    # -------------------------------
    # 5. CREATE DIMENSION TABLES
    # -------------------------------
    create_dimension_tables = SnowflakeOperator(
        task_id="create_dimension_tables",
        snowflake_conn_id="snowflake_default",
        sql="/opt/airflow/project/sql/dimension_tables.sql"
    )

    # -------------------------------
    # 6. CREATE FACT TABLES
    # -------------------------------
    create_fact_tables = SnowflakeOperator(
        task_id="create_fact_tables",
        snowflake_conn_id="snowflake_default",
        sql="/opt/airflow/project/sql/fact_tables.sql"
    )

    # -------------------------------
    # FINAL PIPELINE FLOW
    # -------------------------------
    ingest_raw_data >> run_silver_transformations >> verify_silver_tables >> run_gold_transformations >> create_dimension_tables >> create_fact_tables