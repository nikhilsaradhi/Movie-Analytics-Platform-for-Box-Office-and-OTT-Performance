import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="COMPUTE_WH",
    database="MOVIE_ANALYTICS",
    schema="RAW"
)

cursor = conn.cursor()

print("Uploading files to Snowflake stage...")

cursor.execute("""
PUT file:///opt/airflow/project/pipelines/bronze_layer/raw_tmdb_movies.csv
@MOVIE_STAGE AUTO_COMPRESS=TRUE;
""")

cursor.execute("""
PUT file:///opt/airflow/project/pipelines/bronze_layer/raw_boxoffice.csv
@MOVIE_STAGE AUTO_COMPRESS=TRUE;
""")

cursor.execute("""
PUT file:///opt/airflow/project/pipelines/bronze_layer/raw_imdb_ratings.csv
@MOVIE_STAGE AUTO_COMPRESS=TRUE;
""")

cursor.execute("""
PUT file:///opt/airflow/project/pipelines/bronze_layer/raw_imdb_titles.csv
@MOVIE_STAGE AUTO_COMPRESS=TRUE;
""")

cursor.execute("""
PUT file:///opt/airflow/project/pipelines/bronze_layer/raw_ott_platforms.csv
@MOVIE_STAGE AUTO_COMPRESS=TRUE;
""")

print("All datasets uploaded successfully")

cursor.close()
conn.close()