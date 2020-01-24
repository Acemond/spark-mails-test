from sys import argv

from pyspark.sql import SparkSession

from dataframe import MailRepository, transformations
from graph import grapher

spark = SparkSession.builder \
    .master("local") \
    .appName("BnpExam") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "4")

print("Starting application with CSV at: " + argv[1])
mailRepo = MailRepository(spark)

df = mailRepo.load(argv[1])

sentReceivedDF = transformations.sent_received(df)

mailRepo.save(sentReceivedDF)

top_senders_names = transformations.top_senders_list(sentReceivedDF)

sent_df = transformations.top_senders_sent_count(df, top_senders_names)
distinct_received_df = transformations.top_senders_distinct_recipients_count(df, top_senders_names)

result_df = sent_df.join(distinct_received_df, ["top_sender", "month_year", "month", "year"], "left")\
    .na.fill(0)

grapher.plot_results(result_df)
