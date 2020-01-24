from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import *


def sent_received(df: DataFrame) -> DataFrame:
    exploded_df = df.withColumn("recipient_array", expr("split(recipients, '\\\\|')"))\
        .drop("recipients")\
        .selectExpr("*", "explode(recipient_array) as recipient")\
        .drop(col("recipient_array"))

    sent_df = df.groupBy(col("sender")).agg(count("*").alias("sent"))\
        .withColumnRenamed("sender", "person")
    received_df = exploded_df.groupBy(col("recipient")).agg(count("*").alias("received"))\
        .withColumnRenamed("recipient", "person")

    return sent_df.join(received_df, sent_df["person"] == received_df["person"], "outer").drop(received_df["person"])\
        .na.fill(0)\
        .orderBy(desc("sent"))


def top_senders(mails_df: DataFrame, sent_received_df: DataFrame) -> DataFrame:
    leaders_rows = sent_received_df.limit(10).collect()
    leaders = [str(row["person"]) for row in leaders_rows]

    return mails_df\
        .where(col("sender").isin(leaders))\
        .select("time", "sender", rank().over(Window.partitionBy("sender").orderBy("time")).alias("total_sent"))\
        .orderBy(desc("total_sent"))\
        .selectExpr("sender", "total_sent", "cast(cast(time / 1000 as timestamp) as date) as `date`")\
        .withColumn("month", expr("cast(date_format(date, 'M') as int)"))\
        .withColumn("year", expr("cast(date_format(date, 'yyyy') as int)"))\
        .withColumn("month_year", expr("date_format(date, 'MM/yyyy')"))\
        .drop("date")\
        .groupBy("sender", "month", "year", "month_year").agg(max("total_sent").alias("total_sent"))\
        .withColumnRenamed("max(total_sent)", "total_sent")
