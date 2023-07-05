import json
import math
from datetime import datetime
from botocore.exceptions import ClientError
# now = datetime.now() # current date and time
#
# year = now.strftime("%Y")
# print("year:", year)
#
# month = now.strftime("%m")
# print("month:", month)
#
# day = now.strftime("%d")
# print("day:", day)
#
# time = now.strftime("%H:%M:%S")
# print("time:", time)
#
# date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
# print("date and time:",date_time)
#
# a = "20221031073000"
#
# b = datetime.strptime(a, '%Y%m%d%H%M%S')
#
# print(type(b))
# print(b)
#
#
# begin_time = b.strftime('%yyyy_%mm_%dd_%hh_%mm_%ss')
# print(begin_time)
# print(type(begin_time))
#
# a = datetime.date(datetime.now())
#
# print(a)


#
#
#
#


# try:
#     10/0
# except ZeroDivisionError:
#     print("file not processed")
#
# print("hello world")

#
# a = math.ceil(0 / 500)
# print(a)

# a = []
#
# if a :
#     print("there were number")
# else: print("no number")

d = ['daily_txn_report/data/customer_id=EID-630009840/part-00000-e09a79f3-f833-4fec-bb74-129f506418ff.c000.csv', 'daily_txn_report/data/customer_id=EID-630009962/part-00000-e09a79f3-f833-4fec-bb74-129f506418ff.c000.csv', 'daily_txn_report/data/billing_date=2022-11-19/customer_id=EID-630009977/part-00000-e09a79f3-f833-4fec-bb74-129f506418ff.c000.csv', 'daily_txn_report/data/billing_date=2022-11-19/customer_id=EID-630010592/store_id=LID-630182898/part-00000-e0dce80d-2496-4381-b099-8b98dc395f27.c000.csv', 'daily_txn_report/data/billing_date=2022-11-19/customer_id=EID-630010597/store_id=LID-630182931/part-00000-e0dce80d-2496-4381-b099-8b98dc395f27.c000.csv', 'daily_txn_report/data/billing_date=2022-11-19/customer_id=EID-630010600/store_id=LID-630182936/part-00000-e0dce80d-2496-4381-b099-8b98dc395f27.c000.csv']


a = "temp/oas/market=HK/oas_stamp=2022-11-20/part-00000-bcc71b39-1c46-40db-9090-3049f7c4424f.c000.csv"

b = "temp/daily_txn_report/billing_date=2022-11-19/customer_id_=EID-630009840/part-00000-e09a79f3-f833-4fec-bb74-129f506418ff.c000.csv"

c = "temp/daily_txn_report/billing_date=2022-11-19/customer_id_=EID-630010592/store_id_=LID-630182898/part-00000-e0dce80d-2496-4381-b099-8b98dc395f27.c000.csv"



# import json
# u ="""{"daily_txn_report_script_path":"s3://mwaa-s3-data/dags/txn_processing/daily_transaction_report.py","hudi_txn_base_path":"s3://hudi-write-depo/test_replication/test_data/test_transaction/","hudi_oas_base_path":"s3://hudi-write-depo/test_replication/test_data/test_fee/","hudi_tax_base_path":"s3://hudi-write-depo/test_replication/test_data/test_tax/","temp_daily_txn_report_path":"s3://hudi-write-depo/temp/daily_txn_report/","loc_customers": "['EID-630010592','EID-630010597','EID-630010600']","daily_txn_report_path":"s3://hudi-write-depo/daily_txn_report/data/"}"""
#
# n = json.loads(u)
# print(type(n["loc_customers"]))

# market = a.split('=')[1].split('/')[0]
# partition_path = '/'.join(a.split('/')[2:4])
# print(partition_path)

# customer_id = b.split('=')[1].split('/')[0]
# print(customer_id)
# store_id = b.split('=')[1]
# print(store_id)
#
#
# # for b in d:
# #     customer_id = ''
# #     store_id = ''
# #     if len(b.split('=')) is 3:
# #         customer_id = b.split('=')[1].split('/')[0]
# #         store_id = b.split('=')[2].split('/')[0]
# #     else:
# #         customer_id = b.split('=')[1].split('/')[0]
# #
# #     print(customer_id)
# #     print(store_id)
# b = c
# patrtition = b.split('/')[1]
# print(patrtition)
# customer_id = ''
# store_id = ''
# if len(b.split('=')) == 4:
#     customer_id = b.split('=')[2].split('/')[0]
#     store_id = b.split('=')[3].split('/')[0]
# else:
#     customer_id = b.split('=')[2].split('/')[0]
#
# print(customer_id)
# print(store_id)


# print(datetime.date(datetime.now()))
#
# data = {"key1":None,"key2":"rupesh"}
# print(data.get('key3'))


# d = datetime.strptime('2022-11-24 10:00:00', '%Y-%m-%d %H:%M:%S')
# source_key = 'temp/oas/market=PH/oas_stamp=2022-11-24/part-00000-10a46de7-5cff-4adc-8b9a-fa74697e5f70.c000.csv'
# market = source_key.split('=')[1].split('/')[0]
# partition_path = '/'.join(source_key.split('/')[2:4])
# transformed_date = d.strftime('%Y_%m_%d_%H_%M_%S')
# dest_file_name = 'oas_' + market + '_' + transformed_date + '.csv'
# dest_key = "oas_export/" + partition_path + '/' + dest_file_name
#
# print(dest_key)
#
# f = open(r"C:\Users\krishnan\Downloads\jsontest\part-00005-dde847b2-4ece-4d76-9515-b1db751ba17b-c000.json","r")

#
# a = json.loads(f.read())
#
# print(a)
#
# g = open(r"C:\Users\krishnan\Downloads\jsontest\new.json","w")
# g.write(json.dumps(a))
# g.close()
# f.close()


dat = '2022-11-27 06:30:00'
str_req_date_filename = "_".join(dat.split(" "))
print(str_req_date_filename)