import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import gs_now
from pyspark.sql.functions import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType,StructField,StructType


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# required_columns = ["RUN_ID","SOURCE_TABLE_NAME","RUN_DATE","STATUS","PROCESSED_ROWS"]
table_name = "vendor"

datasyncDf = spark.createDataFrame([table_name],StringType()).withColumnRenamed("value","SOURCE_TABLE_NAME")
datasyncDf = datasyncDf.withColumn("RUN_ID",expr("uuid()"))
datasyncDf = datasyncDf.withColumn("RUN_DATE", current_timestamp())



jdbcUrl = "jdbc:sqlserver://dc3pbxpsql.database.windows.net:1433;databaseName=dc3pbxp-azure;"
username = "mouritechsql"
password = "E9ZitxQhcI+L3J="
query = "select v.*, va.accountname from dbo.vendor v join dbo.vendorAccount va on v.id = va.vendorid and va.id =53"
# new_query = "select v.*,cv.operation from dbo.vendor v join cdc.cdc_vendor cv on v.id = cv.id where cv.is_processed = False
# "

df = spark.read \
  .format("com.microsoft.sqlserver.jdbc.spark") \
  .option("url", jdbcUrl) \
  .option("query", query) \
  .option("user", username) \
  .option("password", password) \
  .load()

data1 = df.withColumn("load_on",current_timestamp())
# data1 = data1.withColumn("is_deleted", when(col("operation") === "DELETE", lit(True)).otherwise(lit(False))).drop("operation")

processed_rows = data1.count()
datasyncDf = datasyncDf.withColumn("PROCESSED_ROWS", lit(processed_rows))
datasyncDf.show(truncate=False)

data1.show(truncate = False)

try:
    # Script for node Amazon Redshift for table data
    FinalDf = DynamicFrame.fromDF(data1, glueContext, "FinalDf")
    AmazonRedshift_node1700068180519 = glueContext.write_dynamic_frame.from_options(
        frame=FinalDf,
        connection_type="redshift",
        connection_options={
            "postactions": "BEGIN; MERGE INTO dbo.vendor USING dbo.vendor_temp_098c12 ON vendor.id = vendor_temp_098c12.id WHEN MATCHED THEN UPDATE SET id = vendor_temp_098c12.id, streamid = vendor_temp_098c12.streamid, vendorshortname = vendor_temp_098c12.vendorshortname, vendorlongname = vendor_temp_098c12.vendorlongname, vendortype = vendor_temp_098c12.vendortype, isinternal = vendor_temp_098c12.isinternal, updated_at = vendor_temp_098c12.updated_at, is_deleted = vendor_temp_098c12.is_deleted WHEN NOT MATCHED THEN INSERT VALUES (vendor_temp_098c12.id, vendor_temp_098c12.streamid, vendor_temp_098c12.vendorshortname, vendor_temp_098c12.vendorlongname, vendor_temp_098c12.vendortype, vendor_temp_098c12.isinternal, vendor_temp_098c12.updated_at, vendor_temp_098c12.is_deleted); DROP TABLE dbo.vendor_temp_098c12; END;",
            "redshiftTmpDir": "s3://aws-glue-assets-665923296292-us-east-1/temporary/",
            "useConnectionProperties": "true",
            "dbtable": "dbo.vendor_temp_098c12",
            "connectionName": "Redshift-Parking-Admin-Redshift",
            "preactions": "CREATE TABLE IF NOT EXISTS dbo.vendor (id INTEGER, streamid VARCHAR, vendorshortname VARCHAR, vendorlongname VARCHAR, vendortype VARCHAR, isinternal BOOLEAN, updated_at TIMESTAMP, is_deleted BOOLEAN); DROP TABLE IF EXISTS dbo.vendor_temp_098c12; CREATE TABLE dbo.vendor_temp_098c12 (id INTEGER, streamid VARCHAR, vendorshortname VARCHAR, vendorlongname VARCHAR, vendortype VARCHAR, isinternal BOOLEAN, updated_at TIMESTAMP, is_deleted BOOLEAN);",
        },
        transformation_ctx="AmazonRedshift_node1700068180519",
    )
except:
    print("error")
else:
    # Script generated for node Amazon Redshift
    FinaldatasyncDf = DynamicFrame.fromDF(datasyncDf, glueContext, "FinaldatasyncDf")
    AmazonRedshift_datasync = glueContext.write_dynamic_frame.from_options(
        frame=FinaldatasyncDf,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": "s3://aws-glue-assets-665923296292-us-east-1/temporary/",
            "useConnectionProperties": "true",
            "dbtable": "dbo.data_sync",
            "connectionName": "Redshift-Parking-Admin-Redshift",
            "preactions": "CREATE TABLE IF NOT EXISTS dbo.data_sync (RUN_ID VARCHAR, SOURCE_TABLE_NAME VARCHAR, RUN_DATE TIMESTAMP, STATUS VARCHAR, INTEGER VARCHAR);",
        },
        transformation_ctx="AmazonRedshift_datasync",
    )

job.commit()
