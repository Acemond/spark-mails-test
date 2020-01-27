from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from dataframe.mail_repository import MailRepository
from dataframe.transformations import *


class Test(TestCase):
    spark = SparkSession.builder.master("local").appName("UnitTestsSpark").getOrCreate()

    def test_sent_received(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", None, "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", None, "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", None, "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", None, "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", None, "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", None, "email")
        ]
        input_df = self.spark.createDataFrame(input_data, MailRepository.explodedSchema)
        result_list = sent_received(input_df).collect()

        self.assertEqual([row["person"] for row in result_list], ["sara shackleton", "mark haedicke", "Greer Mendelow"])
        self.assertEqual([row["sent"] for row in result_list], [3, 2, 1])
        self.assertEqual([row["received"] for row in result_list], [1, 0, 2])

    def test_sent_received_empty(self):

        input_df = self.spark.createDataFrame([], MailRepository.explodedSchema)
        result_list = sent_received(input_df).collect()

        self.assertEqual(result_list, [])

    def test_top_senders_list_empty(self):
        schema = StructType([StructField("person", StringType()), StructField("sent", LongType())])
        df = self.spark.createDataFrame([], schema)
        result = top_senders_list(df, 3)

        self.assertEqual(result, [])

    def test_top_senders_list(self):
        df = self.spark.createDataFrame([("jean", 500), ("michel", 100), ("elsa", 80)], ["person", "sent"])
        result = top_senders_list(df, 3)

        self.assertEqual(result, ["jean", "michel", "elsa"])

    def test_top_senders_sent_count_empty(self):
        input_df = self.spark.createDataFrame([], MailRepository.explodedSchema)
        result = top_senders_sent_count(input_df, [])
        self.assertEqual(result.collect(), [])

    def test_top_senders_sent_count(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", None, "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", None, "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", None, "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", None, "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", None, "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", None, "email")
        ]
        input_df = self.spark.createDataFrame(input_data, MailRepository.explodedSchema)
        result = top_senders_sent_count(input_df, ["sara shackleton", "mark haedicke", "Greer Mendelow"])

        expected = self.spark.createDataFrame([
            ("mark haedicke", 7, 1999, "07/1999", 2),
            ("Greer Mendelow", 7, 1999, "07/1999", 1),
            ("sara shackleton", 7, 1999, "07/1999", 3)
        ], result.schema)

        self.assertCountEqual(result.collect(), expected.collect())

    def test_top_senders_distinct_recipients_count_empty(self):
        input_df = self.spark.createDataFrame([], MailRepository.explodedSchema)
        result = top_senders_sent_count(input_df, [])
        self.assertEqual(result.collect(), [])

    def test_top_senders_distinct_recipients_count(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", None, "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", None, "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", None, "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", None, "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", None, "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", None, "email")
        ]
        input_df = self.spark.createDataFrame(input_data, MailRepository.explodedSchema)
        result = top_senders_distinct_recipients_count(input_df, ["sara shackleton", "mark haedicke", "Greer Mendelow"])

        expected = self.spark.createDataFrame([
            ("Greer Mendelow", 7, 1999, "07/1999", 2),
            ("sara shackleton", 7, 1999, "07/1999", 1)
        ], result.schema)

        self.assertCountEqual(result.collect(), expected.collect())
