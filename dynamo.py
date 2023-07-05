import boto3
dyanmodb = boto3.client('dynamodb', region_name='eu-central-1',
                      aws_access_key_id="ASIAX67SXGJKYQBTQRMK",
                      aws_secret_access_key="pdWG3+TaE8wcajYMMRLzh5BEX4IVuOJJjFM712gP",
                      aws_session_token="IQoJb3JpZ2luX2VjEA8aCXVzLWVhc3QtMSJGMEQCIBRWGBHThF8ALedyEGwlxFlBiWEwtWFGJUZUssGSxIQsAiA2oi7s2Z3iGxsq56ZL+z8HBN6y7N+2buesDCrfGbjEOSqTAwhYEAAaDDU0NzU4MDQ5MDMyNSIMkSHtC++WWXM7miQMKvAC8T9r3JEJ5B/GK7u6GqLhqmPKGD4GfowZ7k4INajQRoabnrTHe7HHiWdlOaH67SyZa2gTx3x4SX1Ye/x7mCD703P1csmWCAlIYkU5GyeGGr1e01SgwMaAwzu5Y4obFloSt+27HOFuq5vUoFptqqlvZGWP9coLeLAbIR4A7MhKFlJA3bKxgNbjn4YAbB1kbduuJe2uTAMEEr1vHsCPp62NSk9KLH1Y/Op1aCE/ejvHxmqqUq22utr1x3Xn3RFzyO8EMmJ0N0e1JGruD6qMM/Gvgy9AAcLI48pEaoThPQAj3MlVNaRsf/OlD/gCkiwQFoIGB6cyFU57oQE004XYEji+zCe4g9LZ8eNIyAHWmGVm2IXQFARD3erxxcdQoWlC+yeXTAUussd4xS3FxKdiXNonfITe2uMn5oLVCSc68EZe9iLDBxARXCx0OF7Q6oQyfXE2zdcYbt/sDm0EkVuIYYoL+eV5k2+vMl79SQT5pDGUAGQw1vS0nQY6pwEFHBXm2dcFao5jS5JxpTVkv721aslCjtswzuq5POSIWJaph9JKpXthoocgF31+dI7aWepJxqW/eqSS/cGyrXemDqp+kHJUH6xaQyIDXNm/N95uvkBXI5Vlt8+bkIBaTFdASU7X5n+tVDhCzcU6vW+DeF2rgH7rfwtAOieFHKDbbE1aC8dnfA0JI9tismGzXXyYxgYDFxJFqZcXViwxHduqLM3N9VLK4w=="
                              )

response = dyanmodb.get_item(
    TableName = 'FX_RATE_dev',
    Key = {
        'partitionKey' : {'S' : 'PHP-MYR'}
    }
)
item = response['Item']

print(item)