import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

jdbcUrl = "jdbc:sqlserver://dc3pbxpsql.database.windows.net:1433/dc3pbxp-azure"
username = "mouritechsql"
password = "E9ZitxQhcI+L3J="

spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .config("spark.jars", "spark-mssql-connector_2.12-1.2.0.jar") \
           .getOrCreate()

print(spark)

df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", jdbcUrl) \
  .option("dbtable", "Address") \
  .option("user", username) \
  .option("password", password) \
  .load()

df.show()
