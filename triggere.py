#
# def spark_job_step( name,
#                    action_on_failure,
#                    spark_args
#                    ):
#     spark_submit_step = [
#         {
#
#             'Name': name,
#             'ActionOnFailure': action_on_failure,
#             'HadoopJarStep':
#                 {
#                     'Jar': 'command-runner.jar',
#                     'Args': spark_args
#                 }
#         }]
#     return spark_submit_step
#
#
# def submit_txn_processing_job(self, raw_location, xcom_file, job_id='spark_submit_txn_processing'):
#         """submit spark job"""
#         hudi_path = self.hudi_base_path
#         hudi_table = self.hudi_table_name
#         hudi_oas_path = self.hudi_oas_base_path
#         if job_id == 'spark_oas_processing':
#             hudi_table = self.hudi_oas_table_name
#         txn_processing_spark_args = [
#             'spark-submit', '--verbose',
#             '--deploy-mode', 'client',
#             '--master', 'yarn',
#             '--jars', self.jars,
#             self.script_path,
#             '--files', "{{ task_instance.xcom_pull('check_files', key='%s') }}" % xcom_file,
#             '--transaction_hudi_base_path', hudi_path,
#             '--transaction_hudi_oas_base_path', hudi_oas_path,
#             '--table', hudi_table,
#             '--merchant_sdb_path', "{{ task_instance.xcom_pull('check_new_files', key='sdb_file') }}",
#             '--raw_location', raw_location,
#             '--run_date', "{{ task_instance.xcom_pull('check_new_files', key='run_date') }}"
#         ]
#         txn_spark_operator = EmrAddStepsOperator(
#             task_id = job_id,
#             job_flow_id = "{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
#             aws_conn_id = 'aws_default',
#             steps = spark_job_step(
#                 'txn_processing',
#                 'TERMINATE_CLUSTER',
#                 txn_processing_spark_args
#             ),
#             dag = self.dag,
#             trigger_rule = 'all_success',
#         )
#         return txn_spark_operator

# from datetime import datetime,timedelta
# a = datetime.date(datetime.now()) - timedelta(5)
#
# print(a)
#
# d = datetime.today() - timedelta(days=5)
# print(d)


