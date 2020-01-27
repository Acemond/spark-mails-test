from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from docs.conf import config


class MailRepository(object):
    CSV_OUTPUT = config["csv_output"]
    spark: SparkSession
    explodedSchema = StructType([
        StructField("time", LongType(), False),
        StructField("messageIdentifier", StringType(), False),
        StructField("sender", StringType()),
        StructField("recipient", StringType(), False),
        StructField("topic", StringType()),
        StructField("mode", StringType()),
        # StructField("called_corrupt_record", StringType()),
    ])

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

        mails_df = self.spark.read\
            .option("escape", "\"")\
            .schema(schema)\
            .csv(input_path)

        return mails_df \
            .withColumn("recipient_array", expr("split(recipients, '\\\\|')")) \
            .drop("recipients") \
            .selectExpr("*", "explode(recipient_array) as recipient") \
            .drop(col("recipient_array"))\
            .cache()

    def save(self, resulting_df: DataFrame):
        resulting_df.coalesce(1).write\
            .option("escape", "\"")\
            .option("quoteAll", "true")\
            .mode("overwrite")\
            .csv(self.CSV_OUTPUT)
