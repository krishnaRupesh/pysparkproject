# # -*- coding: utf-8 -*-
# """
# Created on Tue Jun 15 17:05:20 2021
#
# @author: viswa
# """
#
# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.email_operator import EmailOperator
# from datetime import datetime, timedelta
#
# default_args = {"owner": "airflow", "start_date": datetime(2021, 6, 15)}
#
# with DAG(dag_id="analytics", default_args=default_args, schedule_interval='@daily', catchup=True) as dag:
#     check_file = BashOperator(
#         task_id="check_file",
#         bash_command="shasum ~/ip_files/or.csv",
#         retries=2,
#         retry_delay=timedelta(seconds=15)
#     )
#
#     email = EmailOperator(task_id='send_email',
#                           to='viswatejaster@gmail.com',
#                           subject='Daily report generated',
#                           html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
#                           )
#
#     check_file >> email




