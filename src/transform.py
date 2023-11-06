from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, to_timestamp, struct


def transform_order_batch(streaming_df: DataFrame):
    """
        Orders and filters the DataFrame to get only latest event on batch for an order id
        :param streaming_df: DataFrame
        :return: DataFrame
    """
    transformed_df = streaming_df \
        .withColumn("dat_atui", to_timestamp(col("dat_atui"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
        .select("txt_detl_idt_pedi_pgto", "dat_atui", struct("*").alias("event"))

    latest_events_df = transformed_df \
        .withWatermark("dat_atui", "10 minutes") \
        .groupBy("txt_detl_idt_pedi_pgto") \
        .agg(max("dat_atui").alias("dat_atui"), max("event").alias("event"))

    latest_events_df.printSchema()

    return latest_events_df
