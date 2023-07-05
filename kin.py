import boto3

client = boto3.client('kinesis', region_name='ap-southeast-1',
                      aws_access_key_id="ASIAX67SXGJKROR5RDVZ",
                      aws_secret_access_key="5vaDm/ki4qhtz791dF43jmHbUWC0g+GnRamt1Jem",
                      aws_session_token="IQoJb3JpZ2luX2VjEFUaCXVzLWVhc3QtMSJHMEUCIQCRIf7Ez5L1IEtJQs1X/615zG0os8cLEDFy9Sy1UIIpzgIgWPXTxuwghjGxjvoFChghOL5MqdTt0rY9AF/a/cmI6hUqnAMIjv//////////ARAAGgw1NDc1ODA0OTAzMjUiDOsmoVTX0V6If7w+5SrwAkaGA0EP4EKrvJ7JHnvz7rZAdc/4rLmDg/nrX7sKkjQdgXtHKQULm8vpfSiHPU0OBz20QrNbNnrdHYyICzGUW2+mZek6LJcqbKVv/BVWbRY7B9cWVat/X8K0hEZT+ztlg3h+Y4SAJ+nsKW212c4LzzgtiWhc6Mt6h3x0CgkMMtalf2Bk3JQja2ySfv4CnOOMKWqZ9k0imCcEkx2cywr1unOWrq4sFTfcNfFEJSVqoWY8N+eGHT9xZdShrzAlFpt6hnBfAnFK6Z9wuxyccBlp43H1F1lix96SkAC/fgEBFhOpVzW5V0iXQW5oKxMhwReJSs8AHMO7tr41ziNtOnCUC5E6fp4DFgRTVIdS1r3D6WITiIBhrYGpN4ffSFy//mvjyiAt1PC6Mf+TSzpCl+4q5uZaH0qUf/4SBCvquKQb4L3zeEUlCmaMdQrDab7PNCL1UFvcIq/er1KTwG8rbPurTDUJMPR2oipnUxumI6lbQtiAMIKEjJ0GOqYBru/SakF1AzXS5NtDsryU4BKl+LX7/74JdwWeWs+YS2eRbcQoJdsaBWwjKNXbLnSJ0nt82ih9fiQL/Fx9pz64cGzC1YA3zPhnn7U9aUAnUXVyfS8CWleGLrirSzjrQ6qO77qIglqh7x4Eoltz8gsvgecfc6OUUfr+aRlrQ/vON6EfN4iyaz8smDSXXl5GQ+m6A0Gh4CLc3HI9Bl3B1SJip9Y1g0eJjg==")
a = """
0 -> some other
1 -> 0
2-> 0
3-> 226 ->tax
4 -> 0
5-> 0
6 -> some other
7 -> 0
"""

response1 = client.get_shard_iterator(
    StreamName='Peza-reporting-events',
    ShardId='0',
    ShardIteratorType='TRIM_HORIZON'
)

ShardIterator = response1['ShardIterator']

print(ShardIterator)

# response = client.get_shard_iterator(
#     StreamName='string',
#     ShardId='string',
#     ShardIteratorType='AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
#     StartingSequenceNumber='string',
#     Timestamp=datetime(2015, 1, 1)
# )


response = client.get_records(
    ShardIterator=ShardIterator,
    Limit=10000
)
data = response['Records']
print(len(data))
# print(response['Records'])
required = [i for i in data if i['PartitionKey'] == 'payment']
print(len(required))
print(required[0])
