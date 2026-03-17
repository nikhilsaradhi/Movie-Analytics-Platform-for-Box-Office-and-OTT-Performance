from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("IMDB Data Ingestion") \
        .getOrCreate()
    return spark


def ingest_imdb_data(spark):
    logging.info("Reading IMDB Title Basics")

    basics_df = spark.read.csv(
        "data/raw_data/title.basics.tsv",
        sep="\t",
        header=True,
        inferSchema=True
    )

    logging.info("IMDB Basics Schema")
    basics_df.printSchema()
    basics_df.show(5)

    logging.info("Reading IMDB Ratings")

    ratings_df = spark.read.csv(
        "data/raw_data/ratings.csv",
        header=True,
        inferSchema=True
    )

    logging.info("Ratings Schema")
    ratings_df.printSchema()
    ratings_df.show(5)

    return basics_df, ratings_df


if __name__ == "__main__":
    spark = create_spark_session()
    basics_df, ratings_df = ingest_imdb_data(spark)