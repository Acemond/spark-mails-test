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


def plot_top_senders(top_senders: DataFrame):
    top_senders.cache()

    date_list = create_date_list(top_senders)

    total_df = top_senders.groupBy("sender").agg(sum("mail_count").alias("total"))
    rows = top_senders.join(total_df, "sender") \
        .groupBy("total", "sender")\
        .agg(collect_list(struct("month_year", "mail_count")).alias("sent_this_month"))\
        .orderBy("sender")\
        .collect()

    fig, ax = plt.subplots()
    ax.set_title("Email Senders top")
    ax.set_xlabel("Date")
    ax.set_ylabel("Emails sent")

    for row in rows:
        name = row["sender"] + " (" + str(row["total"]) + ")"
        sent_this_month = row["sent_this_month"]

        date_dict = {item[0]: item[1] for item in sent_this_month}

        result = [date_dict.get(date_list[0])]
        for index, item in enumerate(date_list[1:]):
            result += [date_dict.get(item) or 0]

        ax.plot(date_list, result, label=name)
        ax.legend()

    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("output/plot.png")


def plot_recipients(top_senders_recipients: DataFrame):
    top_senders_recipients.cache()

    date_list = create_date_list(top_senders_recipients)

    total_df = top_senders_recipients.groupBy("recipient").agg(sum("received").alias("total"))
    rows = top_senders_recipients.join(total_df, "recipient") \
        .groupBy("total", "recipient")\
        .agg(collect_list(struct("month_year", "received")).alias("recipients_this_month"))\
        .orderBy("recipient")\
        .collect()

    fig, ax = plt.subplots()
    ax.set_title("Email Senders top inbound distinct recipient count")
    ax.set_xlabel("Date")
    ax.set_ylabel("Emails sent")

    for row in rows:
        name = row["recipient"] + " (" + str(row["total"]) + ")"
        recipients_this_month = row["recipients_this_month"]

        date_dict = {item[0]: item[1] for item in recipients_this_month}

        result = [date_dict.get(date_list[0])]
        for index, item in enumerate(date_list[1:]):
            result += [date_dict.get(item) or 0]

        ax.plot(date_list, result, label=name)
        ax.legend()

    plt.xticks(rotation=90)
    plt.grid()
    plt.tight_layout()
    plt.savefig("output/plot2.png")
