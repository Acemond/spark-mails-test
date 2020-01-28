from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class MailRepository(object):
    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, input_path: str) -> DataFrame:
        schema = StructType([
            StructField("time", LongType(), False),
            StructField("messageIdentifier", StringType(), False),
            StructField("sender", StringType()),
            StructField("recipients", StringType(), False),
            StructField("topic", StringType()),
            StructField("mode", StringType()),
            # StructField("called_corrupt_record", StringType()),
        ])

        mails_df = self.spark.read\
            .option("escape", "\"")\
            .schema(schema)\
            .csv(input_path)

        return mails_df \
            .withColumn("recipient_array", expr("split(recipients, '\\\\|')")) \
            .selectExpr("*", "explode(recipient_array) as recipient") \
            .drop("recipients", "recipient_array", "topic")\
            .cache()

    def save(self, resulting_df: DataFrame, output_path: str):
        resulting_df.coalesce(1).write\
            .option("escape", "\"")\
            .option("quoteAll", "true")\
            .mode("overwrite")\
            .csv(output_path)
