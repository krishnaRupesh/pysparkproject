from datetime import datetime

import boto3

client = boto3.client('kinesis')

response = client.get_shard_iterator(
    StreamName='dev-peza-gateway-events',
    ShardId='1',
    ShardIteratorType='TRIM_HORIZON'
)

# """
# {
#     'Consumer': {
#         'ConsumerName': 'string',
#         'ConsumerARN': 'string',
#         'ConsumerStatus': 'CREATING'|'DELETING'|'ACTIVE',
#         'ConsumerCreationTimestamp': datetime(2015, 1, 2)
#     }
# }
# """
# response = client.register_stream_consumer(
#     StreamARN='arn:aws:kinesis:eu-central-1:547580490325:stream/dev-peza-gateway-events',
#     ConsumerName='emr_python'
# )
#
#
# response = client.subscribe_to_shard(
#     ConsumerARN='string',
#     ShardId='string',
#     StartingPosition={
#         'Type': 'AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER'|'TRIM_HORIZON'|'LATEST'|'AT_TIMESTAMP',
#         'SequenceNumber': 'string',
#         'Timestamp': datetime(2015, 1, 1)
#     }
# )

