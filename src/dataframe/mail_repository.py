from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


class MailRepository(object):
    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, input_path: str):
        schema = StructType([
            StructField("time", LongType(), False),
            StructField("messageIdentifier", StringType(), False),
            StructField("sender", StringType()),
            StructField("recipients", StringType(), False),
            StructField("topic", StringType()),
            StructField("mode", StringType()),
            # StructField("called_corrupt_record", StringType()),
        ])

        return self.spark.read\
            .option("escape", "\"")\
            .schema(schema)\
            .csv(input_path)

    def save(self, resulting_df: DataFrame):
        resulting_df.coalesce(1).write\
            .option("escape", "\"")\
            .option("quoteAll", "true")\
            .mode("overwrite")\
            .csv("output")
