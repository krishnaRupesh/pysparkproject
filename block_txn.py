from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

data = [["1", "02-01-2020 11 01 19 06", "XX84", "100"], ["2", "02-01-2020 11 02 19 406", "XX84", "100"],
        ["3", "02-01-2020 11 03 19 406", "XX84", "104"]]
df2 = spark.createDataFrame(data, ["id", "input", "card_no", "ipaddress"])
df3 = df2.withColumn("timestamp_col", to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS"))
df4 = df3.withColumn("timestamp_col_sec", col("timestamp_col").cast("long"))
df4.show(truncate=False)

card_data_sliding_window = df4.alias("a").join(df4.alias("b"),
                                               (f.col("a.card_no") == f.col("b.card_no")) &
                                               (f.col("b.timestamp_col_sec").between(
                                                   (f.col("a.timestamp_col_sec") -
                                                    1 * 3600),
                                                   f.col("a.timestamp_col_sec"))),
                                               "left") \
    .select("a.id", "a.card_no", "a.ipaddress", "a.timestamp_col",
                                                "a.timestamp_col_sec",
            f.col("b.id").alias("map_np_txn_id"),
            f.col("b.timestamp_col").alias("map_time"),
            f.col("b.ipaddress").alias("map_address"))

card_data_sliding_window.show(truncate=False)

