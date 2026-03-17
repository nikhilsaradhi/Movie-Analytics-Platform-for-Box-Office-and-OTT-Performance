from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("OTT Data Ingestion") \
        .getOrCreate()
    return spark


def ingest_ott_data(spark):
    logging.info("Reading OTT Platforms Dataset")

    df = spark.read.csv(
        "data/raw_data/MoviesOnStreamingPlatforms.csv",
        header=True,
        inferSchema=True
    )

    df=df.drop("_c0")

    logging.info("OTT Dataset Schema")
    df.printSchema()

    logging.info("Sample Data")
    df.show(5)

    return df


if __name__ == "__main__":
    spark = create_spark_session()
    ott_df = ingest_ott_data(spark)