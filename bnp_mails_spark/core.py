from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast

from docs.conf import config
from .dataframe import MailRepository, transformations
from .graph import Grapher


class Application(object):
    VIPS_COUNT = config["vips_count"]
    DISPLAYED_vip_COUNT = config["displayed_vips_count"]
    EXCLUDED_SENDERS = config["excluded_senders"]
    DEFAULT_CSV_INPUT = config["default_csv_input"]
    CSV_OUTPUT = config["csv_output"]
    GRAPH_OUTPUT_FILE = config["graph_output_file"]

    mail_repository: MailRepository
    grapher: Grapher

    def __init__(self):
        spark = SparkSession.builder \
            .master("local") \
            .appName("BnpExam") \
            .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        self.mail_repository = MailRepository(spark)
        self.grapher = Grapher()

    def write_vips(self, mails_df: DataFrame) -> DataFrame:
        sent_received_df = transformations.sent_received(mails_df)
        self.mail_repository.save(sent_received_df, self.CSV_OUTPUT)
        return sent_received_df

    def transform_data(self, mails_df: DataFrame, vips: [str]) -> DataFrame:
        sent_df = transformations.vips_sent_count(mails_df, vips)
        distinct_received_df = transformations.vips_distinct_recipients_count(mails_df, vips)
        return sent_df.join(distinct_received_df, ["vip", "month_year", "month", "year"], "left")\
            .na.fill(0)

    def main(self, user_input_csv: str):
        input_csv_file = user_input_csv or self.DEFAULT_CSV_INPUT
        print("Starting application with CSV at: {}".format(input_csv_file))
        print("Excluding senders: {}".format(self.EXCLUDED_SENDERS))

        mails_df = self.mail_repository.load(input_csv_file)\
            .where(~col("sender").isin(self.EXCLUDED_SENDERS))  # Exclude data

        vips = [row["person"] for row in self.write_vips(mails_df).select("person").take(self.VIPS_COUNT)]
        result_df = self.transform_data(mails_df, vips).cache()

        displayed_df = result_df.where(col("vip").isin(vips[:self.DISPLAYED_vip_COUNT])).cache()
        self.grapher.plot_results(result_df, displayed_df, self.GRAPH_OUTPUT_FILE)
