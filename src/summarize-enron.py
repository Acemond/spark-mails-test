from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dataframe import MailRepository, transformations
from graph import grapher


EXCLUDED_SENDERS = ["pete davis"]
DISPLAYED_TOP_SENDERS_COUNT = 6
TOP_SENDERS_COUNT = 50


spark = SparkSession.builder \
    .master("local") \
    .appName("BnpExam") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")

input_dir = argv[1] or MailRepository.DEFAULT_INPUT_CSV
print("Starting application with CSV at: {}".format(input_dir))
mailRepo = MailRepository(spark)

print("Excluding senders: {}".format(EXCLUDED_SENDERS))

df = mailRepo.load(input_dir)\
    .where(~col("sender").isin(EXCLUDED_SENDERS))  # Exclude data

sentReceivedDF = transformations.sent_received(df)

mailRepo.save(sentReceivedDF)

top_senders_names = transformations.top_senders_list(sentReceivedDF, TOP_SENDERS_COUNT)

sent_df = transformations.top_senders_sent_count(df, top_senders_names)
distinct_received_df = transformations.top_senders_distinct_recipients_count(df, top_senders_names)

result_df = sent_df.join(distinct_received_df, ["top_sender", "month_year", "month", "year"], "left")\
    .na.fill(0)

plot_df = result_df.where(col("top_sender").isin(top_senders_names[:DISPLAYED_TOP_SENDERS_COUNT]))
grapher.plot_results(plot_df, correlation_df=result_df)
