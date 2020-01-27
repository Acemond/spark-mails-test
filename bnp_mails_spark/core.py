from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from .dataframe import MailRepository, transformations
from docs import config
from .graph import Grapher


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

    def main(self, user_input_csv):
        input_csv_file = user_input_csv or config["default_csv_input"]
        print("Starting application with CSV at: {}".format(input_csv_file))
        print("Excluding senders: {}".format(config["excluded_senders"]))

        mails_df = self.mail_repository.load(input_csv_file)\
            .where(~col("sender").isin(config["excluded_senders"]))  # Exclude data

        top_senders_df = self.write_top_senders(mails_df)
        top_senders = transformations.top_senders_list(top_senders_df, config["top_senders_count"])
        result_df = self.transform_data(mails_df, top_senders)

        displayed_df = top_senders[:config["displayed_top_senders_count"]]
        Grapher().plot_results(result_df, displayed_df, config["graph_output_file"])
