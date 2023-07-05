# sdb_path = r"C:\Users\krishnan\Downloads"
# date = '2022-10-21'
# sdb_full_path = sdb_path + f"{''.join(str(date).split('-'))}.csv"
#
# print(sdb_full_path)

# s3://data-547580490325/
source_key = r"temp/daily_txn_report/billing_date_=2022-10-01/customer_id_=EID-630009962/statement_email=ellaine@riway.com;hill@riway.com/part-00000-b4b3d662-5586-45df-83e4-8fc0eba63065.c000.csv"

# data = r"temp/daily_txn_report/billing_date_=2022-11-19/customer_id_=EID-630010600/statement_email=ralph_ong23@yahoo.com/store_id_=LID-630182936/part-00000-7bffc793-4d2d-431d-b3f7-73077d1c9d46.c000.csv"
# source_key = data
partition_path = source_key.split('/')[2]
print(partition_path)
store_id = ''
if len(source_key.split('=')) == 5:
    customer_id = source_key.split('=')[2].split('/')[0]
    print(customer_id)
    email_ids = source_key.split('=')[3].split('/')[0]
    print(email_ids)
    store_id = source_key.split('=')[4].split('/')[0]
    print(store_id)
else:
    customer_id = source_key.split('=')[2].split('/')[0]
    print(customer_id)
    email_ids = source_key.split('=')[3].split('/')[0]
    print(email_ids)

for email_id in email_ids.split(";"):
    dest_file_name = "daily_transaction_report_" + customer_id + "_" + store_id + "_" + email_id + "_" + "20221212131609" + '.csv'
    print(dest_file_name)