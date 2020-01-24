import matplotlib.pyplot as plt
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


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


def plot_top_senders(leaders: DataFrame):
    leaders.cache()

    date_list = create_date_list(leaders)

    rows = leaders.join(leaders.groupBy("sender").agg(max("total_sent").alias("score")), "sender") \
        .groupBy("score", "sender").agg(collect_list(struct("month_year", "total_sent")).alias("sent_by_date"))\
        .orderBy(desc("score"), "sender")\
        .collect()

    fig, ax = plt.subplots()
    ax.set_title("Email Senders top 10")
    ax.set_xlabel("Date")
    ax.set_ylabel("Emails sent")

    for row in rows:
        name = row["sender"] + " (" + str(row["score"]) + ")"
        sent_by_date = row["sent_by_date"]

        date_dict = {item[0]: item[1] for item in sent_by_date}

        result = [date_dict.get(date_list[0])]
        for index, item in enumerate(date_list[1:]):
            result += [date_dict.get(item) or result[index]]

        ax.plot(date_list, result, label=name)
        ax.legend()

    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("output/plot.png")
