import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, collect_list, struct, min, max, sum, desc
from scipy.stats import pearsonr, spearmanr


OUTPUT_FILE = "output/plot.png"


def create_date_list(leaders: DataFrame):
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


def collect_rows(df: DataFrame):
    df.cache()

    total_df = df.groupBy("top_sender")\
        .agg(sum("sent").alias("total_sent"), sum("distinct_recipients").alias("total_distinct_recipients"))

    return df.join(total_df, "top_sender") \
        .groupBy("total_sent", "total_distinct_recipients", "top_sender")\
        .agg(collect_list(struct("month_year", "sent")).alias("sent_this_month"),
             collect_list(struct("month_year", "distinct_recipients")).alias("distinct_recipients_this_month"))\
        .orderBy(desc("total_sent"), "top_sender")\
        .collect()


def plot_sent(rows: [Row], date_list):
    for row in rows:
        name = row["top_sender"] + " (" + str(row["total_sent"]) + ")"
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


def plot_distinct_recipients(rows: [Row], date_list):
    for row in rows:
        name = row["top_sender"] + " (" + str(row["total_distinct_recipients"]) + ")"
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


def plot_correlation(rows: [Row], date_list: [str]):
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
    plt.title("Correlation over {0} senders: {1:.2f} (Pearson), {2:.2f} (Spearman)"
              .format(len(rows), pearson_corr, spearman_corr))
    plt.xlabel("Sent")
    plt.ylabel("Distinct inbound contacts")
    plt.legend()
    plt.xticks(rotation=90)
    plt.grid(True)


def plot_results(df, displayed_senders):
    plot_df = df.where(col("top_sender").isin(displayed_senders))

    plot_rows = collect_rows(plot_df)
    date_list = create_date_list(plot_df)

    plt.figure(figsize=(16, 16))
    plt.subplot(2, 2, 1)
    plot_sent(plot_rows, date_list)

    plt.subplot(2, 2, 2)
    plot_distinct_recipients(plot_rows, date_list)

    correlation_rows = collect_rows(df)
    plt.subplot(2, 2, 3)
    plot_correlation(correlation_rows, date_list)

    plt.tight_layout()
    plt.savefig(OUTPUT_FILE)
