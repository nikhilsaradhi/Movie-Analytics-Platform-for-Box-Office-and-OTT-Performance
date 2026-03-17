from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("TMDB Movies Ingestion") \
        .getOrCreate()
    return spark


def ingest_tmdb_movies(spark):
    logging.info("Reading TMDB Movies Dataset")

    df = spark.read.csv(
        "data/raw_data/tmdb_5000_movies.csv",
        header=True,
        inferSchema=True
    )

    logging.info("TMDB Schema")
    df.printSchema()

    logging.info("Sample Data")
    df.show(5)

    return df


if __name__ == "__main__":
    spark = create_spark_session()
    tmdb_df = ingest_tmdb_movies(spark)