#
# import argparse
#
# from pyspark.sql import SparkSession
# from pyspark import SparkConf
# from pyspark.sql import functions as f
# from functools import reduce
# from pyspark.sql import DataFrame
# import uuid
# from py4j.protocol import Py4JJavaError
# from pyspark.sql.window import Window
#
# class TxnProcessing:
#
#     def __init__(self,
#                  transaction_hudi_base_path,
#                  table,
#                  files,
#                  run_date,
#                  raw_location,
#                  sdb_path=None,
#                  oas_path=None,
#                  app_name='txn_processing'
#                  ):
#         self.files = files
#         self.transaction_hudi_base_path = transaction_hudi_base_path
#         self.table = table
#         self.run_date = run_date
#         self.raw_location = raw_location
#         self.sdb_path = sdb_path
#         self.transaction_hudi_oas_base_path = oas_path
#         self.app_name = app_name
#         self.spark_session = self.get_spark_session()
#
#     def get_spark_session(self):
#         if hasattr(self, 'spark_session'):
#             return self.spark_session
#
#         self.spark_session = SparkSession.builder\
#                                          .config(conf=self.get_spark_config_obj())\
#                                          .appName(self.app_name)\
#                                          .enableHiveSupport()\
#                                          .getOrCreate()
#         self.logger = self.spark_session\
#                           ._jvm.org.apache\
#                           .log4j.LogManager.getLogger(self.app_name)
#         self.spark_session.sparkContext.setCheckpointDir("/tmp")
#         hdf = self.spark_session._jsc.hadoopConfiguration()
#         hdf.set(
#             'mapreduce.input.fileinputformat.split.minsize',
#             str(1024 * 1024 * 128)
#         )
#         hdf.set(
#             'dfs.blocksize',
#             str(1024 * 1024 * 128)
#         )
#         hdf.set(
#             'parquet.block.size',
#             str(1024 * 1024 * 128)
#         )
#         self.logger.info("New SparkSession created.")
#
#         return self.spark_session
#
#     def get_spark_config_obj(self):
#         spark_conf = SparkConf()
#         spark_conf.set("spark.sql.parquet.writeLegacyFormat", "true")
#         spark_conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
#         #spark_conf.set("spark.sql.parquet.enableVectorizedReader", "false")
#         spark_conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#         spark_conf.set("parquet.compression", "SNAPPY")
#         spark_conf.set("yarn.resourcemanager.am.max-attempts", 1)
#         spark_conf.set("spark.yarn.maxAppAttempts", 1)
#         spark_conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
#         spark_conf.set("spark.hadoop.fs.s3.impl",
#                        "com.amazon.ws.emr.hadoop.fs."
#                        "EmrFileSystem")
#         spark_conf.set("spark.hadoop.fs.s3n.impl",
#                        "com.amazon.ws.emr.hadoop.fs.EmrFileSystem")
#         spark_conf.set("hive.metastore.client.factory.class",
#                        "com.amazonaws.glue.catalog."
#                        "metastore."
#                        "AWSGlueDataCatalog"
#                        "HiveClientFactory")
#         spark_conf.set("spark.executor.heartbeatInterval", 50000)
#         return spark_conf
#
#     def get_unique_id(self):
#         id_cond = f.concat(
#             f.lit('NP-'),
#             f.lit(uuid.uuid4().hex).substr(1, 10),
#             f.monotonically_increasing_id(),
#             f.round(f.rand() * f.lit(4294967295)).cast('long'),
#             f.lit(uuid.uuid4().hex)
#         )
#         return id_cond
#
#     def create_unique_transaction_id(self, df):
#         id_cond = self.get_unique_id()
#         df = df.withColumn('nomupay_transaction_id', id_cond.substr(1,30))
#         return df
#
#     def create_group_id(self, df):
#         id_cond = self.get_unique_id()
#         windowGroupIdSpec = Window.partitionBy(f.col('retrieval_reference_number'))
#         df = df.withColumn('ngid', id_cond.substr(1,30))
#         df = df.withColumn("nomupay_group_id", f.first('ngid').over(windowGroupIdSpec)).drop('ngid')
#         return df
#
#     def add_received_date(self, df):
#         df = df.withColumn('transaction_received_at',
#                                            f.lit(self.run_date).cast('timestamp'))
#         return df
#
#     def get_hudi_configurations(self):
#         hudi_options_pp = {
#             'hoodie.table.name': self.table,
#             'hoodie.datasource.write.recordkey.field': "nomupay_transaction_id",
#             'hoodie.datasource.write.partitionpath.field': 'transaction_date',
#             'hoodie.datasource.write.table.name': self.table,
#             'hoodie.datasource.write.operation': 'upsert',
#             'hoodie.datasource.write.precombine.field': 'nomupay_transaction_id',
#             'hoodie.upsert.shuffle.parallelism': 10,
#             'hoodie.insert.shuffle.parallelism': 10,
#             'hoodie.datasource.hive_sync.database': "dwh",
#             'hoodie.datasource.hive_sync.table': self.table,
#             'hoodie.datasource.hive_sync.partition_fields': "transaction_date",
#             'hoodie.datasource.write.hive_style_partitioning': "true",
#             'hoodie.datasource.hive_sync.enable': "true",
#             'hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled': "true",
#             'hoodie.datasource.hive_sync.support_timestamp': "true",
#             'hoodie.metadata.enable': "true"
#         }
#         return hudi_options_pp
#
#     def get_txn_schema(self):
#         hudi_txn = self.spark_session.table("dwh.transaction_schema")
#         return hudi_txn.schema
#
#     def read_hudi(self):
#         is_table_exist = self.spark_session.table("dwh.{}".format(self.table))
#         df = self.spark_session.read.format('hudi').load(self.transaction_hudi_base_path)
#         txn_schema = self.get_txn_schema()
#         columns = []
#         field_dict = {}
#         for field in df.schema.fields:
#             field_dict[field.name] = field.dataType
#         for field in txn_schema.fields:
#             if not field_dict.get(field.name):
#                 columns.append(f.lit(None).alias(field.name).cast(field.dataType))
#             else:
#                 columns.append(field.name)
#         df = df.select(*columns)
#         return df
#
#     def read_oas_hudi(self):
#         df = self.spark_session.read.format('hudi').load(self.transaction_hudi_oas_base_path)
#         return df
#
#     def get_existing_transaction_data(self, df_ai=None, df_oas=None, df_pos=None, df_cs=None):
#         df_hudi = self.read_hudi()
#         if df_cs:
#             df_hudi = df_hudi.select(
#                 f.col('cs_transaction_id').alias('hudi_transaction_id'),
#                 f.col('nomupay_transaction_id').alias('hudi_nomupay_transaction_id'),
#                 f.col('transaction_received_at').alias('hudi_transaction_received_at'),
#                 f.col('oas_stamp').alias('hudi_oas_stamp')
#             )
#             df = df_cs.join(
#                 df_hudi,
#                 df_cs.cs_transaction_id == df_hudi.hudi_transaction_id,
#                 how="left"
#             )
#             df = df.dropDuplicates()
#             txn_id_cond = f.when(
#                 (f.col('hudi_transaction_id') == f.col('cs_transaction_id')),
#                 f.col('hudi_nomupay_transaction_id')
#             ).otherwise(f.col('nomupay_transaction_id'))
#             oas_stamp_cond = f.when(
#                 (f.col('hudi_transaction_id') == f.col('cs_transaction_id')),
#                 f.col('hudi_oas_stamp')
#             ).otherwise(f.col('oas_stamp'))
#             txn_date_cond = f.when(
#                 (f.col('hudi_transaction_id') == f.col('cs_transaction_id')),
#                 f.col('hudi_transaction_received_at')
#             ).otherwise(f.col('transaction_received_at'))
#             df = df.withColumn('nomupay_transaction_id', txn_id_cond)
#             df = df.withColumn('oas_stamp', oas_stamp_cond)
#             df = df.withColumn('transaction_received_at', txn_date_cond).drop(
#                 'hudi_transaction_id',
#                 'hudi_nomupay_transaction_id',
#                 'hudi_transaction_received_at',
#                 'hudi_oas_stamp'
#             )
#             return df
#         if df_pos:
#             df_hudi = df_hudi.select(
#                 f.col('pos_retrieval_reference_number').alias('hudi_retrieval_reference_number'),
#                 f.col('nomupay_group_id').alias('hudi_nomupay_group_id'),
#                 f.col('nomupay_transaction_id').alias('hudi_nomupay_transaction_id'),
#                 f.col('pos_transaction_type').alias('hudi_transaction_type'),
#                 f.col('pos_transaction_date').alias('hudi_transaction_date'),
#                 f.col('pos_transaction_time').alias('hudi_transaction_time'),
#                 f.col('oas_stamp').alias('hudi_oas_stamp'),
#                 f.col('transaction_received_at').alias('hudi_transaction_received_at')
#             )
#             df_hudi_new = df_hudi.select(
#                 f.col('hudi_retrieval_reference_number'),
#                 f.col('hudi_nomupay_group_id')
#             )
#             df = df_pos.join(
#                 df_hudi_new,
#                 df_pos.pos_retrieval_reference_number == df_hudi_new.hudi_retrieval_reference_number,
#                 how="left"
#             )
#             df = df.dropDuplicates()
#             groupid_cond = f.when(f.col('hudi_nomupay_group_id').isNotNull(), f.col('hudi_nomupay_group_id')).otherwise(
#                 f.col('nomupay_group_id'))
#             df = df.withColumn('nomupay_group_id', groupid_cond)
#             df = df.drop('hudi_nomupay_group_id', 'hudi_retrieval_reference_number')
#
#             df = df.join(
#                 df_hudi,
#                 (df.pos_retrieval_reference_number == df_hudi.hudi_retrieval_reference_number) &
#                 (df.pos_transaction_type == df_hudi.hudi_transaction_type) &
#                 (df.pos_transaction_time == df_hudi.hudi_transaction_time) &
#                 (df.pos_transaction_date == df_hudi.hudi_transaction_date),
#                 how="left"
#             )
#
#             txn_id_cond = f.when(
#                 (f.col('hudi_retrieval_reference_number') == f.col('pos_retrieval_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('transaction_type')) &
#                 (f.col('hudi_transaction_date') == f.col('transaction_date')) &
#                 (f.col('hudi_transaction_time') == f.col('transaction_time')),
#                 f.col('hudi_nomupay_transaction_id')
#             ).otherwise(f.col('nomupay_transaction_id'))
#             oas_stamp_cond = f.when(
#                 (f.col('hudi_retrieval_reference_number') == f.col('pos_retrieval_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('transaction_type')) &
#                 (f.col('hudi_transaction_date') == f.col('transaction_date')) &
#                 (f.col('hudi_transaction_time') == f.col('transaction_time')),
#                 f.col('hudi_oas_stamp')
#             ).otherwise(f.col('oas_stamp'))
#             txn_date_cond = f.when(
#                 (f.col('hudi_retrieval_reference_number') == f.col('pos_retrieval_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('transaction_type')) &
#                 (f.col('hudi_transaction_date') == f.col('transaction_date')) &
#                 (f.col('hudi_transaction_time') == f.col('transaction_time')),
#                 f.col('hudi_transaction_received_at')
#             ).otherwise(f.col('transaction_received_at'))
#             df = df.withColumn('nomupay_transaction_id', txn_id_cond)
#             df = df.withColumn('oas_stamp', oas_stamp_cond)
#             df = df.withColumn('transaction_received_at', txn_date_cond)
#             df = df.withColumn('nomupay_group_id', groupid_cond).drop('hudi_nomupay_group_id',
#                                                                       'hudi_retrieval_reference_number',
#                                                                       'hudi_transaction_type',
#                                                                       'hudi_nomupay_transaction_id',
#                                                                       'hudi_transaction_received_at',
#                                                                       'hudi_transaction_date',
#                                                                       'hudi_transaction_time',
#                                                                       'hudi_oas_stamp')
#             return df
#
#         if df_ai:
#             df_hudi = df_hudi.select(
#                 f.col('ai_retrieval_reference_number').alias('hudi_retrieval_reference_number'),
#                 f.col('nomupay_group_id').alias('hudi_nomupay_group_id'),
#                 f.col('nomupay_transaction_id').alias('hudi_nomupay_transaction_id'),
#                 f.col('ai_acquirer_reference_number').alias('hudi_acquirer_reference_number'),
#                 f.col('ai_transaction_type').alias('hudi_transaction_type'),
#                 f.col('oas_stamp').alias('hudi_oas_stamp'),
#                 f.col('transaction_received_at').alias('hudi_transaction_received_at'),
#             )
#             df_hudi_new = df_hudi.select(
#                 f.col('hudi_retrieval_reference_number'),
#                 f.col('hudi_nomupay_group_id')
#             )
#             df = df_ai.join(
#                 df_hudi_new,
#                 df_ai.ai_retrieval_reference_number == df_hudi_new.hudi_retrieval_reference_number,
#                 how="left"
#             )
#             df = df.dropDuplicates()
#             groupid_cond = f.when(f.col('hudi_nomupay_group_id').isNotNull(), df.hudi_nomupay_group_id).otherwise(
#                 df.nomupay_group_id)
#             df = df.withColumn('nomupay_group_id', groupid_cond)
#             df = df.drop('hudi_nomupay_group_id', 'hudi_retrieval_reference_number')
#
#             df = df.join(
#                 df_hudi,
#                 (df.ai_acquirer_reference_number == df_hudi.hudi_acquirer_reference_number) &
#                 (df.ai_transaction_type == df_hudi.hudi_transaction_type),
#                 how="left"
#             )
#             txn_id_cond = f.when(
#                 (f.col('hudi_acquirer_reference_number') == f.col('ai_acquirer_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('ai_transaction_type')),
#                 f.col('hudi_nomupay_transaction_id')
#             ).otherwise(f.col('nomupay_transaction_id'))
#             oas_stamp_cond = f.when(
#                 (f.col('hudi_acquirer_reference_number') == f.col('ai_acquirer_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('ai_transaction_type')),
#                 f.col('hudi_oas_stamp')
#             ).otherwise(f.col('oas_stamp'))
#             txn_date_cond = f.when(
#                 (f.col('hudi_acquirer_reference_number') == f.col('ai_acquirer_reference_number')) &
#                 (f.col('hudi_transaction_type') == f.col('ai_transaction_type')),
#                 f.col('hudi_transaction_received_at')
#             ).otherwise(f.col('transaction_received_at'))
#             df = df.withColumn('nomupay_transaction_id', txn_id_cond)
#             df = df.withColumn('oas_stamp', oas_stamp_cond)
#             df = df.withColumn('transaction_received_at', txn_date_cond).drop(
#                 'hudi_nomupay_group_id',
#                 'hudi_retrieval_reference_number',
#                 'hudi_acquirer_reference_number',
#                 'hudi_transaction_type',
#                 'hudi_nomupay_transaction_id',
#                 'hudi_transaction_received_at',
#                 'hudi_oas_stamp'
#             )
#             return df
#         if df_oas:
#             df_hudi1 = df_hudi.select(
#                 f.col('nomupay_transaction_id').alias('hudi_nomupay_transaction_id'),
#                 f.col('nomupay_group_id').alias('hudi_nomupay_group_id'),
#                 f.col('payout_id').alias('hudi_payout_id')
#             )
#             df = df_oas.join(
#                 df_hudi1,
#                 df_oas.nomupay_parent_transaction_id == df_hudi1.hudi_nomupay_transaction_id,
#                 how='inner')
#             df = df.withColumn('nomupay_group_id', df['hudi_nomupay_group_id']).drop(
#                 'hudi_nomupay_transaction_id',
#                 'hudi_nomupay_group_id',
#             )
#             df = df.withColumn('payout_id', df['hudi_payout_id']).drop('hudi_payout_id')
#             df_hudi_oas = self.read_oas_hudi()
#             df_hudi2 = df_hudi_oas.select(
#                 f.col('nomupay_parent_transaction_id').alias('hudi_nomupay_parent_transaction_id'),
#                 f.col('billing_item').alias('hudi_billing_item'),
#                 f.col('pim_stamp').alias('hudi_pim_stamp'),
#                 f.col('transaction_received_at').alias('hudi_transaction_received_at')
#             )
#             df = df.join(
#                 df_hudi2,
#                 (df.nomupay_parent_transaction_id == df_hudi2.hudi_nomupay_parent_transaction_id) &
#                 (df.billing_item == df_hudi2.hudi_billing_item),
#                 how='left')
#             pim_stamp_cond = f.when(
#                 (f.col('hudi_nomupay_parent_transaction_id') == f.col('nomupay_parent_transaction_id')) &
#                 (f.col('hudi_billing_item') == f.col('billing_item')),
#                 f.col('hudi_pim_stamp')
#             ).otherwise(f.col('pim_stamp'))
#             txn_date_cond = f.when(
#                 (f.col('hudi_nomupay_parent_transaction_id') == f.col('nomupay_parent_transaction_id')) &
#                 (f.col('hudi_billing_item') == f.col('billing_item')),
#                 f.col('hudi_transaction_received_at')
#             ).otherwise(f.col('transaction_received_at'))
#             df = df.withColumn('pim_stamp', pim_stamp_cond)
#             df = df.withColumn('transaction_received_at', txn_date_cond)
#             return df
#
#     def write_raw_location(self, df):
#         df.coalesce(1).write.format('csv').option("header", True).option("delimiter", ",").mode('append').save(
#             self.raw_location)
#
#     def get_sdb_data_frame(self, sdb_path):
#         df_sdb = self.spark_session.read \
#             .format("csv") \
#             .option("header", "true") \
#             .option("delimiter", ",") \
#             .load(sdb_path)
#         return df_sdb
#
#     def get_sdb_data(self):
#         df_sdb = self.get_sdb_data_frame(self.sdb_path)
#         if 'Billing_Profile' in df_sdb.columns:
#             df_sdb = df_sdb.select(
#                 f.col('PAYIN_ID').alias('sdb_payin_id'),
#                 f.col('PAYOUT_ID').alias('sdb_payout_id'),
#                 f.col('Billing_Profile'),
#                 f.col('Merchant_Country').alias('sdb_merchant_country'),
#                 f.col('External_MID'),
#                 f.col('MID'))
#         else:
#             df_sdb = df_sdb.select(
#                 f.col('PAYIN_ID').alias('sdb_payin_id'),
#                 f.col('PAYOUT_ID').alias('sdb_payout_id'),
#                 f.col('Merchant_Country').alias('sdb_merchant_country'),
#                 f.col('External_MID'),
#                 f.col('MID'))
#             df_sdb = df_sdb.withColumn('Billing_Profile', f.lit(None))
#         return df_sdb
#
#     def write_hudi(self):
#         df_hudi = self.processing_logic()
#         print(df_hudi.columns)
#         df_hudi.write.format('hudi').options(**self.get_hudi_configurations()).mode("append").save(
#             self.transaction_hudi_base_path)
#
#     def get_transaction_date(self, df):
#         cond = f.when(
#             f.col('merchant_country') == 'PH', 'Asia/Manila'
#         ).otherwise(
#             f.when(f.col('merchant_country') == 'TH', 'Asia/Bangkok'
#                    ).otherwise(
#                 f.when(
#                     f.col('merchant_country') == 'HK', 'Asia/Hong_Kong'
#                 ).otherwise(
#                     f.when(
#                         f.col('merchant_country') == 'MY', 'Asia/Kuala_Lumpur'
#                     ).otherwise('UTC')
#                 )
#             )
#         )
#         df = df.withColumn('timezone', cond)
#         df = df.withColumn(
#             'transaction_date', f.to_date(f.to_utc_timestamp(df["transaction_date"], df["timezone"])).cast('string')
#         )
#         return df
#
#
# class CardStreamProcessing(TxnProcessing):
#
#     def stamp_date(self, df):
#         stamp_cond = f.when(
#             (df['payin_id'].isNotNull()) &
#             (f.col('transaction_type').isin({'Sale', 'Refund', 'Refund_Sale'})) &
#             (f.col('cs_settled_time').isNotNull()), f.lit(self.run_date).cast('timestamp'))
#         df = df.withColumn('oas_stamp', stamp_cond)
#         return df
#
#     def get_schema(self):
#         cs = self.spark_session.table("dwh.cardstream_raw")
#         return cs.schema
#
#     def add_market(self, cs_df):
#         cs_df = cs_df.withColumn('cs_market', cs_df['merchant_country'])
#         return cs_df
#
#     def add_transaction_type(self, cs_df):
#         message_cond = f.when(cs_df['cs_action'] == "SALE", "Sale").otherwise(
#             f.when(cs_df['cs_action'] == "REFUND_SALE", "Refund").otherwise(
#                 f.when(cs_df['cs_action'] == "REFUND", "Refund").otherwise(
#                     f.when(cs_df['cs_action'] == "VERIFY", "Verify").otherwise(
#                         f.when(cs_df['cs_action'] == "PREAUTH", "PreAuth")
#                     ))))
#         cs_df = cs_df.withColumn('transaction_type', message_cond)
#         return cs_df
#
#     def processing_logic(self):
#         df_raw_cs = self.create_data_frame()
#         cols = [f.regexp_replace(i, ',', '').alias(i) for i in df_raw_cs.columns]
#         df_raw_cs = df_raw_cs.select(*cols)
#         self.write_raw_location(df_raw_cs)
#         df_raw_cs = df_raw_cs.select(*[f.col(i).alias('cs_' + i) for i in df_raw_cs.columns])
#         txn_schema = self.get_txn_schema()
#         columns = []
#         for field in txn_schema.fields:
#             if field.name not in df_raw_cs.columns:
#                 columns.append(f.lit(None).alias(field.name).cast(field.dataType))
#             else:
#                 if str(field.dataType) == 'TimestampType':
#                     columns.append(f.to_timestamp(f.col(field.name), 'yyyy-MM-dd HH:mm:ss').alias(field.name))
#                 else:
#                     columns.append(f.col(field.name).cast(field.dataType))
#         df_cs = df_raw_cs.select(*columns)
#         df_cs = df_cs.dropDuplicates()
#         df_cs = self.create_unique_transaction_id(df_cs)
#         df_sdb = self.get_sdb_data()
#         df_cs = df_cs.join(df_sdb, df_cs.cs_internal_merchant_id == df_sdb.External_MID, how="left").drop('External_MID')
#         df_cs = df_cs.withColumn('payin_id', df_cs['sdb_payin_id']).drop('sdb_payin_id')
#         df_cs = df_cs.withColumn('payout_id', df_cs['sdb_payout_id']).drop('sdb_payout_id')
#         df_cs = df_cs.filter(
#             (f.col('Billing_Profile').isNull()) |
#             (f.col('Billing_Profile') == f.lit('GW'))
#         ).drop('Billing_Profile')
#         df_cs = df_cs.withColumn('merchant_country', f.col('sdb_merchant_country')).drop('sdb_merchant_country')
#         df_cs = df_cs.withColumn('cs_merchant_id', f.col('MID')).drop('MID')
#         df_cs = self.add_market(df_cs)
#         df_cs = self.add_received_date(df_cs)
#         market_txn_date_cond = f.when(
#             f.col('cs_authorised_time').isNotNull(), f.to_date(f.col('cs_authorised_time')).cast('string')).\
#             otherwise(
#             f.to_date(f.col('cs_create_time').cast('string'))
#         )
#         txn_date_cond = f.when(
#             f.col('cs_authorised_time').isNotNull(), f.col('cs_authorised_time').cast('string')). \
#             otherwise(
#             f.col('cs_create_time').cast('string')
#         )
#         df_cs = df_cs.withColumn('transaction_date', txn_date_cond)
#         df_cs = self.get_transaction_date(df_cs)
#         df_cs = df_cs.withColumn('market_transaction_date', market_txn_date_cond)
#         source_cond = f.when(f.col('source').isNull(), f.lit('CS')).otherwise(f.lit(None))
#         df_cs = df_cs.withColumn("source", source_cond)
#         df_cs = self.stamp_date(df_cs)
#         df_cs = self.add_transaction_type(df_cs)
#         df_cs = df_cs.withColumn('transaction_amount', f.col('cs_amount_received'))
#         df_cs = df_cs.withColumn('currency_code', f.col('cs_currency_code'))
#         df_cs = df_cs.withColumn('merchant_id', f.col('cs_merchant_id'))
#         df_cs = df_cs.withColumn('card_type', f.col('cs_card_type'))
#         df_cs = self.get_existing_transaction_data(df_cs=df_cs)
#         columns = [field.name for field in txn_schema.fields]
#         df_cs = df_cs.select(*columns)
#         return df_cs
#
#     def create_data_frame(self):
#         df_cs = self.spark_session.read \
#             .format("csv") \
#             .option("header", "true") \
#             .option("delimiter", ",") \
#             .schema(self.get_schema()) \
#             .load(self.files)
#         return df_cs
