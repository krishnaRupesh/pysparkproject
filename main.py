# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql.types import MapType, StringType, IntegerType,FloatType,DoubleType,StructField,StructType,BooleanType

from pyspark.sql.functions import col, when, concat_ws, lit, from_json

from pyspark.sql.functions import col, when, concat_ws, lit, from_json

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
#
#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


# spark = SparkSession.builder.getOrCreate()
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# # data1 = [["1", "02-01-2020 11 01 19 06", "XX84", "100", "1"], ["2", "02-01-2020 11 02 19 406", "XX84", "100", "1"],
# #         ["3", "02-01-2020 11 03 19 406", "YX84", "104", "3"]]
# #
# data = [[1, "20221110050957", "20221110050957123", "XX84", "100", "1;100"], [2, "20221110050957", "20221110", "XX84", "100", "3;100"],
#         [3, "20221110050957", "20221110050957", "YX84", "104", "3;104"]]
# df2 = spark.createDataFrame(data, ["id", "input", "new_input", "card_no", "ipaddress", "new_id"])

# df2.show()

# df3 = df2.withColumn("is_transaction", lit(True))
#
# df3.show()
# df3.printSchema()

# df3 = df2.withColumn("odd_even", when(col("id")%2 == 0, lit("even")).when(col("id")%2 == 1, lit("odd")).otherwise(lit("undefined")))
#
# df3.show()
#
# divisible_by both 2 and 4
# is_divisible by 4
# is_divisiber by 2,
# not_divisibal by anything

# df3 = df2.withColumn("timesamp", to_timestamp(col("input"),"yyyyMMddHHmmss"))
# df3 = df2.withColumn("timesamp", (col("input").cast(DoubleType())))
# df3.show()
# df3.printSchema()

# arrayStructureData = [
#     (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
# ]
#
# arrayStructureSchema = StructType([
#     StructField('name', StructType([
#         StructField('firstname', StringType(), True),
#         StructField('middlename', StringType(), True),
#         StructField('lastname', StringType(), True)
#     ])),
#     StructField('languages', ArrayType(StringType()), True),
#     StructField('state', StringType(), True),
#     StructField('gender', StringType(), True)
# ])
# #
# df = spark.createDataFrame(data=arrayStructureData, schema=arrayStructureSchema)
# df.printSchema()
# df.show(truncate=False)
#
# df.filter(df.state == "OH") \
#     .show(truncate=False)
# #
# df.filter(col("state") == "OH") \
#     .show(truncate=False)
#
# df.filter("gender  == 'M'") \
#     .show(truncate=False)
# #
# df.filter((df.state == "OH") | (df.gender == "M")) \
#     .show(truncate=False)
# #
# df.filter(array_contains(df.languages, "Java")) \
#     .show(truncate=False)
# #
# df.filter(df.name.lastname == "Williams") \
#     .show(truncate=False)
#
# df.filter(df.gender != 'M').show()
# df.filter(("df.gender <> 'M'")).show()
#
# states = ["OH","NY"]
# df.filter(~(df.state.isin(states)))
#
# df.filter(df.state.like("%O%"))
#
# df.sort("state","gender").show()


# df.select(df.name,explode_outer(df.languages)).show()
#
# a = ["java","scala",None,"beta"]
#
# print(len(a))





























# dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
#   (('Michael','Rose',''),'2000-05-19','M',4000),
#   (('Robert','','Williams'),'1978-09-05','M',4000),
#   (('Maria','Anne','Jones'),'1967-12-01','F',4000),
#   (('Jen','Mary','Brown'),'1980-02-17','F',-1)
# ]
#
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType
# schema = StructType([
#         StructField('name', StructType([
#              StructField('firstname', StringType(), True),
#              StructField('middlename', StringType(), True),
#              StructField('lastname', StringType(), True)
#              ])),
#          StructField('dob', StringType(), True),
#          StructField('gender', StringType(), True),
#          StructField('salary', IntegerType(), True)
#          ])
#
# df = spark.createDataFrame(data = dataDF, schema = schema)
#
# df1 = df.withColumnRenamed("dob","DateOfBirth") # rdd (DAG)
#
# df1.printSchema()
#
# df2 = df1.withColumnRenamed("dob","DateOfBirth").withColumnRenamed("salary","salary_amount")
# df2.printSchema()
#
#
#
# from pyspark.sql.functions import *
# df.select(col("name.firstname").alias("fname"), \
#   col("name.middlename").alias("mname"), \
#   col("name.lastname").alias("lname"), \
#   col("dob"),col("gender"),col("salary")) \
#   .printSchema()
#
# df.select(col("dob"),col("gender"),col("salary")).show()
#
#
# #
#
# df4 = df.withColumn("fname",col("name.firstname")) \
#       .withColumn("mname",col("name.middlename")) \
#       .withColumn("lname",col("name.lastname")) \
#       .drop("name")
# df4.printSchema()
# #
# newColumns = ["newCol1","newCol2","newCol3","newCol4"]
# df.toDF(*newColumns).printSchema()
#
#
#
#
#
#
#
#
#
#
#







































# df2.show()

# journey = spark.read.option("header","true").csv(r"C:\Users\krishnan\Downloads\JOURNETSELECTED_20240117_124251.csv")
#
# assetjourney = journey.select("ASSETID").distinct()
#
# view = spark.read.option("header","true").csv(r"C:\Users\krishnan\Downloads\selectjourneyview_20240117_124611.csv")
# assetview = view.select("ASSETID").distinct()
#
# data = assetjourney.join(assetview, assetview.ASSETID == assetjourney.ASSETID, "left_anti")
#
# data.show(100,truncate=False)
#
# newdata = journey.join(data,journey.ASSETID == data.ASSETID)
#
# print(newdata.count())

# scandata = spark.read.option("header","true")\
#     .csv(r"C:\Users\krishnan\Downloads\selectscandata_20240117_133141.csv")
# scandata.printSchema()
# data = scandata.first()
# print(type(data))
# print(data)
# print(type(data["BARCODE"]))
# print(data["BARCODE"])
# print(type(data["ISMANUALLYPROCESSED"]))
# print(data["ISMANUALLYPROCESSED"])
# print(type(data["ASSETTYPE"]))
# print(data["ASSETTYPE"])
# newdata = scandata.select("BARCODE","ASSETTYPE").distinct()
#
# print(newdata.count())
#
# newdata.show(100,truncate=False)
# df3 = df2.withColumn("timestamp_col", to_timestamp(col("input"), "yyyyMMddHHmmss"))
# df4 = df2.withColumn("timestamp_col1", to_timestamp(col("new_input"), "yyyyMMddHHmmssSSS"))
# df5 = df4.withColumn("reg_formet",  date_format(col("timestamp_col1"), "yyyy-MM-dd hh:mm:ss"))
#
# df3.show(truncate=False)
# df4.show(truncate=False)
# df5.show(truncate=False)

# df2.show(truncate=False)
# import datetime
#
# x = datetime.datetime.now()


# df3 = df2.withColumn("dfsfs", lit(datetime.datetime.now()))
# df3.show(truncate=False)
# df3.printSchema()
# a = df3.select(collect_list('id')).first()[0]
#
# print(a)
# print(type(a))
# print(tuple(a))
# df2.filter(~(lower(col('card_no')).contains('xx'))).show(truncate=False)

# df2.select(col("input").alias("Newer input"), col('card_no').alias('Newer Card_no')).coalesce(1).write.mode("overwrite") \
#     .option("header", "true").csv(r'C:\Users\krishnan\Downloads\daily_data')
#
# df4 = df3.withColumn("req_time", date_format(col("timestamp_col"), "yyyy-MM-dd hh:mm:ss"))
#
# df5 = df3.withColumn("Name_Array", split(col("new_id"), ";"))
# df5.show(truncate=False)

# df6 = df5.select("*", explode(col("Name_Array")))
# df6.show(truncate=False)


# df6 = df5.filter(array_contains(col("Name_Array"),"100"))
# df6.show(truncate=False)
#
# cols = ['id','input']
#
# req_cols = [col(i).alias("good_"+ i) for i in cols]
#
#
# df6.select('Name_Array', *req_cols).show(truncate=False)

# df3.write.mode("overwrite").option("ignoreNullFields", "false").json(r"C:\Users\krishnan\Downloads\jsontest")
# df4.show(truncate=False)
# df4.write.mode("overwrite").option("ignoreNullFields", "false").json(r"C:\Users\krishnan\Downloads\jsontest4")
#

# condition = when(col('ipaddress') == '100', col('ipaddress')).otherwise(lit('120'))

# df4 = df3
#
# df4.show(truncate=False)
#
#
# def add_condition(df, condition, column_name):
#     col_condition = f.when(
#         condition,
#         col(column_name)
#     ).otherwise(col("new_" + column_name))
#     df = df.withColumn("very"+column_name, col_condition)
#     return df
#
#
# req_columns = ['id','input']
#
# cs_condition = (col("id") == col("new_id"))
#
# for i in req_columns:
#     df4 = add_condition(df4, cs_condition, i)
#
# df4.show(truncate=False)









# df4 = df3.withColumn("data", struct(col("id").alias("nomupay_transaction_id"),
#                                                  col('input').alias('transaction_date'),
#                                                  col('card_no').alias('card_no'),
#                                                  col('ipaddress').alias('ipaddress'),
#                                                  col('timestamp_col').alias('timestamp_col'),
#                                                  col('new_col').alias('new_col')
#                                                  )).withColumn("data", to_json(col("data"), {"ignoreNullFields":"false"})).select("data")
# # df3.show(truncate=False)
# df4.write.mode("overwrite").csv(r"C:\Users\krishnan\Downloads\jsontest1")

#
# df3.withColumn("checl", lit("2022-11-19 22:40:00").cast('date')).show(truncate=False)

# df3.coalesce(1).write.mode("overwrite").option("header","true").partitionBy("ipaddress").csv(r"C:\Users\krishnan\Downloads\tester1")
# df3.groupby("ipaddress").pivot("id").agg(first("input").alias("req_one"),first("timestamp_col").alias("req_two")).show(truncate=False)
# df4 = df3.withColumn("data",struct(df3.columns)).withColumn("data", to_json(col("data"))).select("data")
# df4.printSchema()
# df4.show(truncate=False)
# df4 = df3.select("timestamp_col").orderBy("timestamp_col").limit(1)
# time_hours = 2
# df5 = df4.withColumn('timestamp_col', col("timestamp_col") - expr(f"INTERVAL {time_hours} HOURS"))
# df5.show(truncate=False)

# data2 = [["-2", "02-01-2020 10 00 19 06", "XX84", "100"], ["-1", "02-01-2020 11 00 19 06", "XX84", "100"], ["1", "02-01-2020 11 01 19 06", "XX84", "100"], ["2", "02-01-2020 11 02 19 406", "XX84", "100"],
#         ["3", "02-01-2020 11 03 19 406", "XX84", "104"]]
#
# datadf2 = spark.createDataFrame(data2, ["id", "input", "card_no", "ipaddress"])
# datadf3 = datadf2.withColumn("timestamp_col", to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS"))
# datadf3.show(truncate=False)
#
#
#
# datadf3.join(df4,datadf3.timestamp_col < df4.timestamp_col, "leftsemi").show(truncate=False)

















# df5  = df4.withColumnRenamed("card_no","b_card_no").withColumnRenamed("timestamp_col_sec","b_timestamp_col_sec")\
#     .withColumn("one_hr",col("b_timestamp_col_sec") - 3600).withColumnRenamed("id","map_np_txn_id")\
#     .withColumnRenamed("timestamp_col","map_time").withColumnRenamed("ipaddress","map_address")
#
# df5.show()


# df4.join(df5,(df4.card_no == df5.b_card_no) & ((df4.timestamp_col_sec).between(df5.one_hr,df5.b_timestamp_col_sec))).show(truncate=False)

# df4.join(df5,(df4.card_no == df5.b_card_no) & ((df4.timestamp_col_sec > df5.one_hr) & (df4.timestamp_col_sec <= df5.b_timestamp_col_sec))).show(truncate=False)

# & (df4.timestamp_col_sec > df5.b_timestamp_col_sec))
# from datetime import datetime, date
# # import pandas as pd
# from pyspark.sql import Row

#
# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])
# df.show()


# txns = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Desktop\\HOUSE\\txns.csv")
# # fees = spark.read.parquet("C:\\Users\\krishnan\\Downloads\\nomupay_data\\fees4")
# # sdb_df = spark.read.option("header", "true").csv("C:\\Users\\krishnan\\Downloads\\nomupay_data\\ow_20220705.csv")
# selected_columns = [column for column in txns.columns if column.startswith("cs_")]
# txn1 = txns.filter(col("cs_transaction_id").isNotNull()).select(selected_columns)
# txn1.show()
# txn1.select("cs_settled_time","cs_authorised_time","cs_create_time").show()
#
# txn_time_cond = f.when(f.col('cs_settled_time').isNotNull() & ~(f.col('cs_settled_time') == '""'),
#                        f.to_timestamp(f.col('cs_settled_time')).cast('string')).otherwise(
#     f.when(f.col('cs_authorised_time').isNotNull() & ~(f.col('cs_authorised_time') == '""'),
#            f.to_timestamp(f.col('cs_authorised_time')).cast('string')).otherwise(
#         f.to_timestamp(f.col('cs_create_time').cast('string'))))
#
# txn2 = txn1.withColumn('transaction_time', txn_time_cond)
#
# txn2.show(20,False)
# txn2.select("cs_settled_time","cs_authorised_time","cs_create_time","transaction_time").show(20,False)
#

# txns.coalesce(1).write.mode("overwrite").option("header","true").csv("C:\\Users\\krishnan\\Downloads\\nomupay_data\\txn101")

# mid is unique
# df = txns.filter(col('cs_card_number').isNotNull()).filter(col('transaction_time').isNotNull()). \
#     filter(col('cs_remote_address').isNotNull()) \
#     .select("cs_card_number", "transaction_time", "cs_remote_address") \
#     .withColumn("transaction_time", to_timestamp(col("transaction_time"), "MM/dd/yyyy HH:mm")) \
#     .withColumn("transaction_time_in_sec", col("transaction_time").cast("long"))
#
# df.filter(f.col("cs_card_number") == "524302XXXXXX0025").orderBy(desc("transaction_time")).show()
#
# df.alias("a").join(df.alias("b"),
#                    (f.col("a.cs_card_number") == f.col("b.cs_card_number")) &
#                    (f.col("a.transaction_time_in_sec").between((f.col("b.transaction_time_in_sec") - 3600),
#                                                                f.col("b.transaction_time_in_sec"))), "left") \
#     .select("a.cs_card_number", "a.cs_remote_address", "a.transaction_time",
#             col("b.transaction_time").alias("map_time"),col("b.cs_remote_address").alias("map_address")).groupby("cs_card_number").agg(
#     collect_set("map_address").alias("map_address_list"),countDistinct("map_address").alias("map_address_count")).filter(f.col('map_address_count') > 1).show(200,False)
#


# d1 = txns.filter(col('cs_card_number').isNotNull()).filter(col('transaction_time').isNotNull()).filter(
#     col('cs_remote_address').isNotNull()).select("cs_card_number", "transaction_time", "cs_remote_address")
#
# d1.withColumn("transaction_time", to_timestamp(col("transaction_time"), "MM/dd/yyyy HH:mm")).withColumn("lol", col(
#     "transaction_time").cast("long") - 3600).show()

# data2 = d1.join(d2.select("transaction_time", "cs_card_number"), (d1.cs_card_number == d2.cs_card_number) &
#                 (d1.transaction_time.between(d2.transaction_time, d2.transaction_time-1*(3600))), "left")\
#     .select(d1.transaction_time,d1.cs_card_number,d1.cs_remote_address, d2.cs_remote_address.alias("new_cs_remote_address"),d2.transaction_time.alias("new_transaction_time"))


#
# # txns.show()
# txns = txns.filter(f.col("transaction_time").isNotNull()).filter(f.col("cs_remote_address").isNotNull()) \
#     .withColumn("transaction_time", f.to_timestamp("transaction_time"))
#
# windowSpec = Window.partitionBy("cs_card_number").orderBy("transaction_time")
#
# card_data_hudi_df = txns.withColumn("lag_ipaddress", f.lag("cs_remote_address").over(windowSpec)).withColumn(
#     "lag_transaction_time", f.lag("transaction_time").over(windowSpec))
#
# card_data_hudi_df = card_data_hudi_df \
#     .withColumn("same_ip_address",
#                 f.when(f.col("cs_remote_address") == f.col("lag_ipaddress"), f.lit(True)).otherwise(False)) \
#     .withColumn("time_diff",
#                 (f.col("transaction_time").cast("long") - f.col("lag_transaction_time").cast("long")) / 3600)
#
# risk = card_data_hudi_df.filter(f.col("same_ip_address") == False & f.col("time_diff") < 1.0)
#
# card_data_hudi_df.printSchema()
# card_data_hudi_df.show()
#
# #
# # card_brand_cond = f.when(f.lower(f.col('ai_card_type')).startswith('visa'), f.lit('VISA')) \
# #             .when(f.lower(f.col('ai_card_type')).startswith('mastercard'), f.lit('MC')).otherwise(f.lit('Unknown'))
# #
# # df = sdb_df.withColumn('card_brand', card_brand_cond)


# txn2 = txns.withColumn("test", lit('test')).withColumn('test1', lit(None)).select('test','test1')
#
# txn3 = txn2.withColumn('test3',when(col('test') == col('test1'), lit('Y')).otherwise(lit('N')))
# txn3.show()
#
# txn3.printSchema()


# data = spark.readStream.format("kinesis").option("streamName", "dev-peza-gateway-events").option("endpointUrl", "https://kinesis.eu-central-1.amazonaws.com").option("startingposition", "TRIM_HORIZON").load()
# data1 = data.selectExpr("CAST(data as STRING) as jsdata")
# data1.withColumn("data1",from_json(data1.jsdata,MapType(StringType(),StringType()))).writeStream.format("console").option("truncate",False).outputMode("append").start().awaitTermination()
# #
# sdb_df.printSchema()
# sdb_df.withColumnRenamed("Account_Owner_Name", "External_MID").write
# cols = txns.columns
#
# dd =     [x for x in cols if x.startswith("cs")]
# print(dd)
#
# # print(txns.count())
# txns.filter(col("source") == "CS").show(20,False)
# txns.alias("").join(fees,col("txns.nomupay_transaction_id") ==col("fees.nomupay_parent_transaction_id")).show()

# txns = txns.withColumn("merchant_name", col("cs_merchant_name")) \
#     .withColumn("transaction_currency", col("currency_code")) \
#     .withColumn("payment_method", when(col("pos_payment_method").isNull(), col("cs_payment_method")).otherwise(
#     col("pos_payment_method"))) \
#     .withColumn("mid", col("merchant_id")) \
#     .withColumn("shop_id", col("payin_id"))
#
# data = fees.alias("fees").join(txns.alias("txn"),
#                                fees.nomupay_parent_transaction_id == txns.nomupay_transaction_id, "left") \
#     .select(col("fees.nomupay_transaction_id"), col("fees.transaction_date"),
#             col("fees.nomupay_parent_transaction_id"), col("txn.merchant_name"), col("txn.transaction_amount"),
#             col("txn.transaction_currency"), col("txn.transaction_type"), col("txn.payment_method"),
#             col("txn.mid"), col("txn.shop_id")).withColumn("sortkey",
#                                                            concat_ws("#", lit("fee"),
#                                                                      col("fees.transaction_date"),
#                                                                      col("fees.nomupay_parent_transaction_id"),
#                                                                      col("fees.nomupay_transaction_id")))
#
# data2 = data.alias("data").join(sdb_df.alias("sdb").select("MID", "Customer_ID", "Account_Owner_Name"),
#                                 data.mid == sdb_df.MID, "left").select("data.*", "sdb.Customer_ID", "sdb.Account_Owner_Name").withColumn("merchant_name",col("sdb.Account_Owner_Name"))
#
#
# data2.show()


# pyspark - -jars / usr / share / aws / aws - java - sdk / aws - java - sdk - bundle - 1.12
# .31.jar, / usr / lib / spark / external / lib / spark - avro.jar, / usr / lib / hudi / hudi - spark - bundle.jar, s3: // dynamodb - schemas / dynamodb - connector / jars / spark - dynamodb_2
# .12 - 1.1
# .2.jar - -conf
# 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
# cs_data = spark.read.option("header", "true").csv("s3://hudi-write-depo/card_stream/")
#
# cs_data = cs_data.withColumn("Cardholder Address", regexp_replace(col("Cardholder Address"), ",", ""))
#
# txn_data = spark.read.format("hudi").load("s3://hudi-write-depo/txnv102/")
#
# required_cols = txn_data.columns
# txn_data = txn_data.filter(col("source") == "CS")
#
# join_data = txn_data.alias("txn").join(cs_data.alias("cs"), col('txn.cs_transaction_id') == col('cs.Transaction ID'),
#                                        "left")
#
# new_data = join_data \
#     .withColumn("cs_create_time", col("Create Time").cast(TimestampType())) \
#     .withColumn("cs_modify_time", col("Modify Time").cast(TimestampType())) \
#     .withColumn("cs_user_id", col("User ID")) \
#     .withColumn("cs_user_name", col("User Name")) \
#     .withColumn("cs_internal_merchant_id", col("Merchant ID")) \
#     .withColumn("cs_merchant_name", col("Merchant Name")) \
#     .withColumn("cs_customer_name", col("Customer Name")) \
#     .withColumn("cs_processor_name", col("Processor Name")) \
#     .withColumn("cs_integration_name", col("Integration Name")) \
#     .withColumn("cs_action", col("Action")) \
#     .withColumn("cs_method", col("Method")) \
#     .withColumn("cs_country_code", col("Country Code")) \
#     .withColumn("cs_currency_code", col("Currency Code")) \
#     .withColumn("cs_currency_number", col("Currency Number")) \
#     .withColumn("cs_amount_required", col("Amount Required").cast(FloatType())) \
#     .withColumn("cs_amount_approved", col("Amount Approved").cast(FloatType())) \
#     .withColumn("cs_amount_received", col("Amount Received").cast(FloatType())) \
#     .withColumn("cs_amount_refunded", col("Amount Refunded").cast(FloatType())) \
#     .withColumn("cs_cross_reference", col("Cross Reference")) \
#     .withColumn("cs_unique_reference", col("Unique Reference")) \
#     .withColumn("cs_order_description", col("Order Description")) \
#     .withColumn("cs_cardholder_name", col("Cardholder Name")) \
#     .withColumn("cs_cardholder_address", col("Cardholder Address")) \
#     .withColumn("cs_cardholder_postcode", col("Cardholder Postcode")) \
#     .withColumn("cs_cardholder_email", col("Cardholder Email")) \
#     .withColumn("cs_payment_method", col("Payment Method")) \
#     .withColumn("cs_card_type", col("Card Type")) \
#     .withColumn("cs_card_brand", col("Card Brand")) \
#     .withColumn("cs_card_number", col("Card Number")) \
#     .withColumn("cs_card_expiry", col("Card Expiry")) \
#     .withColumn("cs_3ds_check", col("3DS Check")) \
#     .withColumn("cs_cv2_check", col("CV2 Check")) \
#     .withColumn("cs_address_check", col("Address Check")) \
#     .withColumn("cs_postcode_check", col("PostCode Check")) \
#     .withColumn("cs_risk_check", col("Risk Check")) \
#     .withColumn("cs_remote_address", col("Remote Address")) \
#     .withColumn("cs_auth_code", col("Auth Code")) \
#     .withColumn("cs_chargebacks", col("Chargebacks")) \
#     .withColumn("cs_state", col("State")) \
#     .withColumn("cs_response_code", col("Response Code")) \
#     .withColumn("cs_response_message", col("Response Message")) \
#     .withColumn("cs_capture_delay", col("Capture Delay")) \
#     .withColumn("cs_authenticated_time", col("Authenticated Time").cast(TimestampType())) \
#     .withColumn("cs_authorised_time", col("Authorised Time").cast(TimestampType())) \
#     .withColumn("cs_captured_time", col("Captured Time").cast(TimestampType())) \
#     .withColumn("cs_settled_time", col("Settled Time").cast(TimestampType())) \
#     .withColumn("cs_refunded_time", col("Refunded Time").cast(TimestampType())) \
#     .select(required_cols)
#
#
#
# hudi_options_pp = {
#     'hoodie.datasource.write.recordkey.field': "nomupay_transaction_id",
#     'hoodie.datasource.write.partitionpath.field': 'transaction_date',
#     'hoodie.datasource.write.table.name': "txnv10",
#     'hoodie.datasource.write.operation': 'upsert',
#     'hoodie.datasource.write.precombine.field': 'nomupay_transaction_id',
#     'hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled': "true",
#     'hoodie.metadata.enable': "true",
#     'hoodie.upsert.shuffle.parallelism': 10,
#     'hoodie.insert.shuffle.parallelism': 10,
#     'hoodie.table.name': "txnv10"
#
# }
#
# new_data.write.format('hudi').options(**hudi_options_pp).mode("append").save("s3://hudi-write-depo/txnv102/")




# spark = SparkSession.builder.getOrCreate()

# df = spark.read.parquet(r"C:\Users\krishnan\Downloads\sparkoutputs\zipcodes")
# df.printSchema()
#
# df.show()
#
# parquet -> columanar data
# not sorage
# binaryornot
#
# 99




# df = spark.read.options(header='True').csv(r"C:\Users\krishnan\Downloads\zipcodes.csv")
# df.printSchema()
#
# # df.show()
#
#
# # df.write.csv(r"C:\Users\krishnan\Downloads\sparkoutputs\zipcodes")
# # #
# # df.write.options(header='True', delimiter=',').csv(r"C:\Users\krishnan\Downloads\sparkoutputs\zipcodes")
# # #
# df.write.partitionBy("Country","State").mode('overwrite').options(header='True', delimiter='|').csv(r"C:\Users\krishnan\Downloads\sparkoutputs\zipcodes") # append ,ignore, error, overwrite
# #
# #


# df.write.parquet(r"C:\Users\krishnan\Downloads\sparkoutputs\zipcodes_parquest")
#
#

# multiple folders -> folder with country name
#
#
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#
# spark = SparkSession.builder.appName('SparkByExamples.com').master("local[5]").getOrCreate()
#
# emp = [(1, "Smith", -1, "2018", "10", "M", 3000), \
#        (2, "Rose", 1, "2010", "20", "M", 4000), \
#        (3, "Williams", 1, "2010", "10", "M", 1000), \
#        (4, "Jones", 2, "2005", "10", "F", 2000), \
#        (5, "Brown", 2, "2010", "40", "", -1), \
#        (6, "Brown", 2, "2010", "50", "", -1) \
#        ]
# empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", \
#               "emp_dept_id", "gender", "salary"]
#
# empDF = spark.createDataFrame(data=emp, schema=empColumns)
# empDF.printSchema()
# empDF.show(truncate=False)
#
# dept = [("Finance", 10), \
#         ("Marketing", 20), \
#         ("Sales", 30), \
#         ("IT", 40) \
#         ]
# deptColumns = ["dept_name", "dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)

# empDF.join(broadcast(deptDF), empDF.emp_dept_id == deptDF.dept_id, "inner") \
#     .show(truncate=False)
#
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer") \
#     .show(truncate=False)
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full") \
#     .show(truncate=False)
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter") \
#     .show(truncate=False)
#
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left") \
#     .show(truncate=False)
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter") \
#     .show(truncate=False)
#
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "right") \
#     .show(truncate=False)
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "rightouter") \
#     .show(truncate=False)
#
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi") \
#     .show(truncate=False)
#
# empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti") \
#     .show(truncate=False)

# empDF.alias("emp1").join(empDF.alias("emp2"), \
#                          col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
#     .select(col("emp1.emp_id"), col("emp1.name"), \
#             col("emp2.emp_id").alias("superior_emp_id"), \
#             col("emp2.name").alias("superior_emp_name")) \
#     .show(truncate=False)
#
# empDF.createOrReplaceTempView("EMP")
# deptDF.createOrReplaceTempView("DEPT")
#
#
#
# joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
#     .show(truncate=False)
#
# joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
#     .show(truncate=False)



# empDF.groupby("gender").agg(count("*").alias("gender_count")).show(truncate=False)
#
# empDF.groupby("superior_emp_id").agg(collect_list("name").alias("employess_list")).show(truncate=False)
#




# from pyspark.sql.window import Window
#
# def1 = Window.partitionBy("emp_dept_id").orderBy("salary")
#
# df2 = empDF.withColumn("lead", lead("salary").over(def1)).show()




# data  = spark.read.json(r"C:\Users\krishnan\Downloads\zipcodes.json")
# data.printSchema()
# data.show()

# df = spark.read.format('org.apache.spark.sql.json').load(r"C:\Users\krishnan\Downloads\zipcodes.json")
#
# multiline_df = spark.read.option("multiline","true") \
#       .json(r"C:\Users\krishnan\Downloads\multiline-zipcode.json")
# multiline_df.show()
#
#
# schema = StructType([
#       StructField("RecordNumber",IntegerType(),True),
#       StructField("Zipcode",IntegerType(),True),
#       StructField("ZipCodeType",StringType(),True),
#       StructField("City",StringType(),True),
#       StructField("State",StringType(),True),
#       StructField("LocationType",StringType(),True),
#       StructField("Lat",DoubleType(),True),
#       StructField("Long",DoubleType(),True),
#       StructField("Xaxis",IntegerType(),True),
#       StructField("Yaxis",DoubleType(),True),
#       StructField("Zaxis",DoubleType(),True),
#       StructField("WorldRegion",StringType(),True),
#       StructField("Country",StringType(),True),
#       StructField("LocationText",StringType(),True),
#       StructField("Location",StringType(),True),
#       StructField("Decommisioned",BooleanType(),True),
#       StructField("TaxReturnsFiled",StringType(),True),
#       StructField("EstimatedPopulation",IntegerType(),True),
#       StructField("TotalWages",IntegerType(),True),
#       StructField("Notes",StringType(),True)
#   ])
#
# df_with_schema = spark.read.schema(schema) \
#         .json(r"C:\Users\krishnan\Downloads\zipcodes.json")
# df_with_schema.printSchema()
# df_with_schema.show()
#
#
# jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
# df=spark.createDataFrame([(1, jsonString)],["id","value"])
# df.show(truncate=False)
#
# df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
# df2.printSchema()
# df2.show(truncate=False)
#
# df2.withColumn("value",to_json(col("value"))).show(truncate=False)
#
# from pyspark.sql.functions import json_tuple
# df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
#     .toDF("id","Zipcode","ZipCodeType","City") \
#     .show(truncate=False)

from pyspark.sql.functions import get_json_object
# df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
#     .show(truncate=False)



# nested_df = spark.read.json(r"C:\Users\krishnan\Downloads\nested_json.json")
#
# nested_df.show()
# nested_df.printSchema()
#
# nested_df.select("City.city_name").show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#
spark = SparkSession.builder.appName('SparkByExamples.com').master("local[5]").getOrCreate()

# df = spark.range(0,20)
# print(df.rdd.getNumPartitions())
#
# df.write.csv(r"C:\Users\krishnan\Downloads\sparkoutput")
#
# df.coalesce(10)


data = spark.read.json(r"C:\Users\krishnan\Downloads\997718cb-f559-4965-8087-706eae577e38.json")
# data.select("movementId").show(200,truncate=False)
data1 = data.withColumn("PREVSCANQTY", when(col("quantity") > 0, col("quantity")).otherwise(lit(None)))\
    .withColumn("SCANQTY", when(col("quantity") < 0,(col("quantity") * -1)).otherwise(lit(None)))\
    .select("movementId","PREVSCANQTY","SCANQTY")

data1.show(200,truncate=False)

# data.groupby("movementId").agg(collect_list("quantity")).show(200,truncate=False)
#
# data.write.mode("overwrite").option("header","true").csv(r"C:\Users\krishnan\Downloads\csv")

data1.write.mode("overwrite").option("header","true").csv(r"C:\Users\krishnan\Downloads\csv1")



