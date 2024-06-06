from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.jars", "spark-mssql-connector_2.12-1.2.0.jar").getOrCreate()

jdbcUrl = "jdbc:sqlserver://dc3pbxpsql.database.windows.net;databaseName=dc3pbxp-azure;"
username = "mouritechsql"
password = "E9ZitxQhcI+L3J="

df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", jdbcUrl) \
  .option("dbtable", "address") \
  .option("user", username) \
  .option("password", password) \
  .load()

df.show()
# df = spark.read.option("inferSchema","true").csv(r"C:\Users\krishnan\Downloads\IMPALLECONEU_REPAIR.csv")
# df.show()

