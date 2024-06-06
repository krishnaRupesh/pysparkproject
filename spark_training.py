
s3_file_list = ['s3://reimagined-bi-studio/CoupaFiles/Invoice/DW_Invoice_20240227_030403Z.csv', 's3://reimagined-bi-studio/CoupaFiles/Invoice/DW_Invoice_20240228_030312Z.csv']
for s3_files in s3_file_list:

    try:
        file_name = s3_files.split('/')[-1]
        print(file_name)
    except:
        pass

DW_Invoice_UUID.csv

header_DW_Invoice_UUID.json
