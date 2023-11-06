import os

from pyspark.sql import SparkSession


import transform
from write import Writer
from read import Reader

INPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../input_events/")


def start(spark: SparkSession) -> None:
    streaming_batch_df = Reader(spark, INPUT_PATH).read()
    latest_events_df = transform.transform_order_batch(streaming_batch_df)
    Writer(spark).write(latest_events_df)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Streaming Process Files") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .master("local[*]") \
        .getOrCreate()

    # To allow automatic schemaInference while reading
    spark.conf.set("spark.sql.streaming.schemaInference", True)
    start(spark)
