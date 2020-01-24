import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, struct, min, max, sum


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


def plot_results(result_df):
    result_df.cache()

    date_list = create_date_list(result_df)

    total_df = result_df.groupBy("top_sender")\
        .agg(sum("sent").alias("total_sent"), sum("distinct_recipients").alias("total_distinct_recipients"))

    rows = result_df.join(total_df, "top_sender") \
        .groupBy("total_sent", "total_distinct_recipients", "top_sender")\
        .agg(collect_list(struct("month_year", "sent")).alias("sent_this_month"),
             collect_list(struct("month_year", "distinct_recipients")).alias("distinct_recipients_this_month"))\
        .orderBy("top_sender")\
        .collect()

    plt.figure(figsize=(10, 10))
    plt.subplot(2, 1, 1)
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

    plt.subplot(2, 1, 2)
    for row in rows:
        name = row["top_sender"] + " (" + str(row["total_distinct_recipients"]) + ")"
        distinct_recipients_this_month = row["distinct_recipients_this_month"]

        recipients_dict = {item[0]: item[1] for item in distinct_recipients_this_month}

        result = []
        for index, item in enumerate(date_list):
            result += [recipients_dict.get(item) or 0]

        plt.plot(date_list, result, label=name)

    plt.title("Top Senders distinct recipients")
    plt.xlabel("Date")
    plt.ylabel("Distinct recipients")
    plt.legend()
    plt.xticks(rotation=90)
    plt.grid(True)

    plt.tight_layout()
    plt.savefig("output/plot.png")
