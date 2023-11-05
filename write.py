from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, explode
import os
import table_transformer


def is_first_run():
    try:
        is_empty = (len(os.listdir("tables/intermediate_table/")) == 0)
    except FileNotFoundError:
        return True
    return is_empty


class Writer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_tables(self, batch_df: DataFrame, target_df: DataFrame) -> None:
        """
        Builds and writes tables according to the schema of the raw JSON event
        overwrites output paths with newly generated table containing the latest events
        :param batch_df: DataFrame
        :param target_df: DataFrame
        :return: None
        """

        # clear cache is needed to read and write to the same location
        self.spark.catalog.clearCache()
        merged_df = batch_df.join(target_df, "txt_detl_idt_pedi_pgto", "outer") \
            .where((batch_df.dat_atui >= target_df.dat_atui) | (target_df.dat_atui.isNull())) \
            .select(batch_df.txt_detl_idt_pedi_pgto, batch_df.dat_atui,
                    coalesce(batch_df.event, target_df.event).alias("event"))

        # trigger action for cache before write
        merged_df.cache().show(1)

        # Write the updated intermediate table with the latest events
        merged_df.write.format("parquet").mode("overwrite").save("tables/intermediate_table")

        # Build Customer table
        customer = table_transformer.CustomerTransformer().transform(df=merged_df.select("event.*"))
        customer.write.format("parquet").mode("overwrite").save("tables/customer")
        customer.show()

        # Build DeliveryAddress table
        delivery_address, df_with_id = table_transformer.DeliveryAddressTransformer().transform(
            df=merged_df.select("event.*"))
        delivery_address.write.format("parquet").mode("overwrite").save("tables/delivery_address")
        delivery_address.show()

        # collects delivery_id generated
        merged_df = merged_df.join(df_with_id, on="txt_detl_idt_pedi_pgto")

        # Build Seller table
        seller = table_transformer.SellerTransformer().transform(df=merged_df.select("event.*")
                                                                 .select(explode("list_item_pedi"))
                                                                 .select("col.*"))
        seller.write.format("parquet").mode("overwrite").save("tables/seller")
        seller.show()

        # Build Item table
        item = table_transformer.ItemTransformer().transform(df=merged_df.select("event.*")
                                                             .select(explode("list_item_pedi"))
                                                             .select("col.*"))
        item.write.format("parquet").mode("overwrite").save("tables/item")
        item.show()

        # Build Promotion table
        promotion = table_transformer.PromotionTransformer().transform(df=merged_df.select("event.*")
                                                                       .select(explode("list_prmo"))
                                                                       .select("col.*"))
        promotion.write.format("parquet").mode("overwrite").save("tables/promotion")
        promotion.show()

        # Build Shipment table
        shipment = table_transformer.ShipmentTransformer().transform(df=merged_df \
                                                                     .select("event.*")
                                                                     .select(explode("list_envo"))
                                                                     .select("col.*"))
        shipment.write.format("parquet").mode("overwrite").save("tables/shipment")
        shipment.show()

        # Build ItemShipment table
        item_shipment = table_transformer.ItemShipmentTransformer().transform(df=merged_df.select("event.*")
                                                                              .select(explode("list_envo"))
                                                                              .select("col.*")
                                                                              .select(explode("list_item_envo"))
                                                                              .select("col.*"))
        item_shipment.write.format("parquet").mode("overwrite").save("tables/item_shipment")
        item_shipment.show()

        # Build FactOrderItem table
        order_item = table_transformer.FactOrderItemTransformer().transform(
            df=merged_df.select("event.*", "delivery_address_id")
            .withColumn("item_exploded",
                        explode("list_item_pedi"))
            .drop("list_item_pedi").
            select("*", "item_exploded.*"))
        order_item.write.format("parquet").mode("overwrite").save("tables/order_item")
        order_item.show()

    def collect_target_df(self, df: DataFrame) -> DataFrame:
        """
         Collects current DataFrame of latest events
         from intermediate_table
        :param df: DataFrame
        :return: DataFrame
        """
        if not is_first_run():
            return self.spark.read.format("parquet").option("multiLine", True).load("tables/intermediate_table/")
        else:
            return self.spark.createDataFrame([], schema=df.schema)

    def write(self, latest_events_df: DataFrame) -> None:
        """
        For every batch of new events builds modeled tables
        :param latest_events_df: 
        :return: None
        """
        latest_events_df.writeStream \
            .format("parquet") \
            .option("path", "tables/intermediate_table/") \
            .option("checkpointLocation", "checkpoint") \
            .outputMode("complete") \
            .trigger(processingTime="30 seconds") \
            .foreachBatch(lambda df, batch_id: self.build_tables(df, self.collect_target_df(df))) \
            .start() \
            .awaitTermination()
