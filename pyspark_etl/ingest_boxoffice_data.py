from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Box Office Data Ingestion") \
        .getOrCreate()
    return spark


def ingest_boxoffice_data(spark):
    logging.info("Reading Box Office Dataset")

    df = spark.read.csv(
        "data/raw_data/Mojo_budget_update.csv",
        header=True,
        inferSchema=True
    )

    logging.info("Box Office Schema")
    df.printSchema()

    logging.info("Sample Data")
    df.show(5)

    return df


if __name__ == "__main__":
    spark = create_spark_session()
    boxoffice_df = ingest_boxoffice_data(spark)