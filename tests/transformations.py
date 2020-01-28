from unittest import TestCase

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, Row, StructField, LongType, StringType

from bnp_mails_spark.dataframe.transformations import *


class Test(TestCase):
    spark = SparkSession.builder.master("local").appName("UnitTestsSpark").getOrCreate()
    vips_df = spark.createDataFrame([Row("sara shackleton"), Row("mark haedicke"), Row("Greer Mendelow")], ["person"])
    explodedSchema = StructType([
        StructField("time", LongType(), False),
        StructField("messageIdentifier", StringType(), False),
        StructField("sender", StringType()),
        StructField("recipient", StringType(), False),
        StructField("mode", StringType()),
    ])

    def test_sent_received(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", "email")
        ]
        input_df = self.spark.createDataFrame(input_data, self.explodedSchema)
        result_list = sent_received(input_df).collect()

        self.assertEqual([row["person"] for row in result_list], ["sara shackleton", "mark haedicke", "Greer Mendelow"])
        self.assertEqual([row["sent"] for row in result_list], [3, 2, 1])
        self.assertEqual([row["received"] for row in result_list], [1, 0, 2])

    def test_sent_received_empty(self):
        input_df = self.spark.createDataFrame([], self.explodedSchema)
        result_list = sent_received(input_df).collect()

        self.assertEqual(result_list, [])

    def test_vips_sent_count_empty(self):
        input_df = self.spark.createDataFrame([], self.explodedSchema)
        result = vips_sent_count(input_df, self.vips_df)
        self.assertEqual(result.collect(), [])

    def test_vips_sent_count(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", "email")
        ]
        input_df = self.spark.createDataFrame(input_data, self.explodedSchema)
        result = vips_sent_count(input_df, self.vips_df)

        expected = self.spark.createDataFrame([
            ("mark haedicke", 7, 1999, "07/1999", 2),
            ("Greer Mendelow", 7, 1999, "07/1999", 1),
            ("sara shackleton", 7, 1999, "07/1999", 3)
        ], ["vip", "month", "year", "month_year", "sent"])

        self.assertCountEqual(result.collect(), expected.collect())

    def test_vips_distinct_recipients_count_empty(self):
        input_df = self.spark.createDataFrame([], self.explodedSchema)
        result = vips_sent_count(input_df, self.vips_df)
        self.assertEqual(result.collect(), [])

    def test_vips_distinct_recipients_count(self):
        input_data = [
            (930852480000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1A@zlsvr22>", "sara shackleton", "paige grumulaitis", "email"),
            (930852580000, "<K1N2P4FDZ24EIZ5UYHTPMPPSZTIWR5G1Y@zlsvr22>", "sara shackleton", "scott sefton", "email"),
            (930852720000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5B@zlsvr22>", "mark haedicke", "scott sefton", "email"),
            (930852760000, "<PBDB4ORDF1FTP4GCQ0V2ILDDQ30HCMU5D@zlsvr22>", "mark haedicke", "Greer Mendelow", "email"),
            (930855120000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5B@zlsvr22>", "sara shackleton", "Greer Mendelow", "email"),
            (930855140000, "<JK521PRJ0ADHGTLLYIXF0HDWHC2OEML5C@zlsvr22>", "Greer Mendelow", "sara shackleton", "email")
        ]
        input_df = self.spark.createDataFrame(input_data, self.explodedSchema)
        result = vips_distinct_recipients_count(input_df, self.vips_df)

        expected = self.spark.createDataFrame([
            ("Greer Mendelow", 7, 1999, "07/1999", 2),
            ("sara shackleton", 7, 1999, "07/1999", 1)
        ], ["vip", "month", "year", "month_year", "sent"])

        self.assertCountEqual(result.collect(), expected.collect())
