from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dataframe import MailRepository, transformations
from graph import grapher


EXCLUDED_SENDERS = ["pete davis"]
TOP_SENDERS_COUNT = 10


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

grapher.plot_results(result_df)
