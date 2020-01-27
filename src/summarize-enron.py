from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from dataframe import MailRepository, transformations
from graph import grapher


EXCLUDED_SENDERS = ["pete davis"]
DISPLAYED_TOP_SENDERS_COUNT = 6
TOP_SENDERS_COUNT = 50


class Application(object):
    mail_repository: MailRepository

    def __init__(self):
        spark = SparkSession.builder \
            .master("local") \
            .appName("BnpExam") \
            .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        self.mail_repository = MailRepository(spark)

    def write_top_senders(self, mails_df):
        sent_received_df = transformations.sent_received(mails_df)
        self.mail_repository.save(sent_received_df)
        return sent_received_df

    def transform_data(self, mails_df, top_senders):
        sent_df = transformations.top_senders_sent_count(mails_df, top_senders)
        distinct_received_df = transformations.top_senders_distinct_recipients_count(mails_df, top_senders)

        return sent_df.join(distinct_received_df, ["top_sender", "month_year", "month", "year"], "left")\
            .na.fill(0)

    def main(self, input_csv, excluded):
        mails_df = self.mail_repository.load(input_csv)\
            .where(~col("sender").isin(excluded))  # Exclude data

        top_senders_df = self.write_top_senders(mails_df)
        top_senders = transformations.top_senders_list(top_senders_df, TOP_SENDERS_COUNT)
        result_df = self.transform_data(mails_df, top_senders)

        grapher.plot_results(result_df, top_senders[:DISPLAYED_TOP_SENDERS_COUNT])


inputCsvFile = argv[1] or MailRepository.DEFAULT_INPUT_CSV
print("Starting application with CSV at: {}".format(inputCsvFile))
print("Excluding senders: {}".format(EXCLUDED_SENDERS))
Application().main(inputCsvFile, EXCLUDED_SENDERS)
