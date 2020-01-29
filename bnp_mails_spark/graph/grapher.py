import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, collect_list, struct, min, max, sum, desc
from scipy.stats import pearsonr, spearmanr


class Grapher(object):
    def __create_date_list(self, leaders: DataFrame) -> [str]:
        min_year = leaders.select(min("year")).first()[0]
        min_month = leaders.where(col("year") == min_year).select(min("month")).first()[0]
        max_year = leaders.select(max("year")).first()[0]
        max_month = leaders.where(col("year") == max_year).select(max("month")).first()[0]

        dates = []
        for month in range(min_month, 13):
            dates += [(month, min_year)]

        if min_year != max_year:
            for year in range(min_year + 1, max_year):
                for month in range(1, 13):
                    dates += [(month, year)]

        for month in range(1, max_month + 1):
            dates += [(month, max_year)]

        return [str(date[0]).rjust(2, "0") + "/" + str(date[1]) for date in dates]

    def __collect_rows(self, df: DataFrame) -> [Row]:
        df.cache()

        total_df = df.groupBy("vip")\
            .agg(sum("sent").alias("total_sent"), sum("distinct_recipients").alias("total_distinct_recipients"))

        return df.join(total_df, "vip") \
            .groupBy("total_sent", "total_distinct_recipients", "vip")\
            .agg(collect_list(struct("month_year", "sent")).alias("sent_this_month"),
                 collect_list(struct("month_year", "distinct_recipients")).alias("distinct_recipients_this_month"))\
            .orderBy(desc("total_sent"), "vip")\
            .collect()

    def __plot_sent(self, rows: [Row], date_list: [str]):
        for row in rows:
            name = row["vip"] + " (" + str(row["total_sent"]) + ")"
            sent_this_month = row["sent_this_month"]

            sent_dict = {item[0]: item[1] for item in sent_this_month}

            result = []
            for index, item in enumerate(date_list):
                result += [sent_dict.get(item) or 0]

            plt.plot(date_list, result, label=name)

        plt.title("Top senders sent mails")
        plt.xlabel("Date")
        plt.ylabel("Sent mails")
        plt.legend()
        plt.xticks(rotation=90)
        plt.grid(True)

    def __plot_distinct_recipients(self, rows: [Row], date_list: [str]):
        for row in rows:
            name = row["vip"] + " (" + str(row["total_distinct_recipients"]) + ")"
            distinct_recipients_this_month = row["distinct_recipients_this_month"]

            distinct_recipients_dict = {item[0]: item[1] for item in distinct_recipients_this_month}

            result = []
            for index, item in enumerate(date_list):
                result += [distinct_recipients_dict.get(item) or 0]

            plt.plot(date_list, result, label=name)

        plt.title("Top senders distinct inbound contacts")
        plt.xlabel("Date")
        plt.ylabel("Distinct inbound contacts")
        plt.legend()
        plt.xticks(rotation=90)
        plt.grid(True)

    def __plot_correlation(self, rows: [Row], date_list: [str]):
        sent = []
        recip = []

        for row in rows:
            distinct_recipients_this_month = row["distinct_recipients_this_month"]
            sent_this_month = row["sent_this_month"]

            distinct_recipients_dict = {item[0]: item[1] for item in distinct_recipients_this_month}
            sent_dict = {item[0]: item[1] for item in sent_this_month}
            for index, item in enumerate(date_list):
                sent += [sent_dict.get(item) or 0]
                recip += [distinct_recipients_dict.get(item) or 0]

        plt.scatter(sent, recip)

        pearson_corr, _ = pearsonr(sent, recip)
        spearman_corr, _ = spearmanr(sent, recip)
        plt.title("Correlation over top {0} senders: {1:.2f} (Pearson), {2:.2f} (Spearman)"
                  .format(len(rows), pearson_corr, spearman_corr))
        plt.xlabel("Sent")
        plt.ylabel("Distinct inbound contacts")
        plt.grid(True)

    def plot_results(self, df: DataFrame, plot_df: DataFrame, output_file: str):
        plot_rows = self.__collect_rows(plot_df)
        date_list = self.__create_date_list(plot_df)

        plt.figure(figsize=(18, 18))
        plt.subplot(2, 2, 1)
        self.__plot_sent(plot_rows, date_list)

        plt.subplot(2, 2, 2)
        self.__plot_distinct_recipients(plot_rows, date_list)

        correlation_rows = self.__collect_rows(df)
        plt.subplot(2, 2, 3)
        self.__plot_correlation(plot_rows, date_list)

        plt.subplot(2, 2, 4)
        self.__plot_correlation(correlation_rows, date_list)

        plt.tight_layout()
        plt.savefig(output_file)
