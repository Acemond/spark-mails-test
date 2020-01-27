from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def sent_received(mails_df: DataFrame) -> DataFrame:
    sent_df = mails_df.groupBy(col("sender")).agg(count("messageIdentifier").alias("sent"))\
        .withColumnRenamed("sender", "person")
    received_df = mails_df.groupBy(col("recipient")).agg(count("*").alias("received"))\
        .withColumnRenamed("recipient", "person_r")

    return sent_df.join(received_df, sent_df["person"] == received_df["person_r"], "outer").drop(col("person_r"))\
        .where("person IS NOT NULL")\
        .na.fill(0)\
        .orderBy(desc("sent"))  # .withColumn("rank", row_number().over(Window().orderBy(desc("sent"))))


def top_senders_list(sent_received_df: DataFrame, top_senders_count: int):
    top_senders_rows = sent_received_df.limit(top_senders_count).collect()
    return [str(row["person"]) for row in top_senders_rows]


def top_senders_sent_count(mails_df: DataFrame, top_senders_names: [str]):
    return mails_df \
        .where(col("sender").isin(top_senders_names))\
        .selectExpr("*", "cast(cast(time / 1000 as timestamp) as date) as `date`")\
        .withColumn("month", expr("cast(date_format(date, 'M') as int)"))\
        .withColumn("year", expr("cast(date_format(date, 'yyyy') as int)"))\
        .withColumn("month_year", expr("date_format(date, 'MM/yyyy')"))\
        .groupBy("sender", "month", "year", "month_year").agg(count("messageIdentifier").alias("sent"))\
        .withColumnRenamed("sender", "top_sender")


def top_senders_distinct_recipients_count(mails_df: DataFrame, top_senders_names: [str]):
    return mails_df.where(col("recipient").isin(top_senders_names))\
        .selectExpr("*", "cast(cast(time / 1000 as timestamp) as date) as `date`") \
        .withColumn("month", expr("cast(date_format(date, 'M') as int)")) \
        .withColumn("year", expr("cast(date_format(date, 'yyyy') as int)")) \
        .withColumn("month_year", expr("date_format(date, 'MM/yyyy')")) \
        .groupBy("recipient", "month", "year", "month_year").agg(countDistinct("sender").alias("distinct_recipients"))\
        .withColumnRenamed("recipient", "top_sender")
