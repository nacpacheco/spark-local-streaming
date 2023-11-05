from pyspark.sql import DataFrame, SparkSession


class Reader:
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path

    def read(self) -> DataFrame:
        """
        Creates the streaming batch DatFrame from input directory
        :return: DataFrame
        """
        return self.spark.readStream \
            .format("json") \
            .option("multiLine", True) \
            .option("maxFilesPerTrigger", 10) \
            .load(self.path)
