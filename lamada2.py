import json
import boto3
from datetime import datetime, timedelta


def lambda_handler(event, context):
    client = boto3.client('athena')
    req_date = datetime.date(datetime.now()) - timedelta(5)
    req_query = f"""
                SELECT pos_retrieval_reference_number AS retrieval_reference_number,
                   pos_currency_code              AS transaction_currency,
                   pos_transaction_amount         AS transaction_amount,
                   'Reversal'                     AS transaction_type,
                   pos_merchant_id                AS merchant_id,
                   pos_card_number                AS card_number,
                   transaction_date               AS transaction_date
                FROM   dwh.transactions1 a
                   left join (SELECT retrieval_reference_number AS join_column
                              FROM   dwh.transactions1
                              WHERE  transaction_type = 'Reversal') b
                          ON a.pos_retrieval_reference_number == b.join_column
                WHERE  pos_retrieval_reference_number IS NOT NULL
                   AND pos_transaction_type = 'Sale'
                   AND ai_retrieval_reference_number IS NULL
                   AND join_column IS NULL
                   AND transaction_date < '{req_date}' 
 """
    response = client.start_query_execution(
        QueryString=req_query,
        QueryExecutionContext={
            'Database': 'dwh'
        },
        ResultConfiguration={
            'OutputLocation': 's3://hudi-write-depo/athena_output/',
        },
        WorkGroup='primary'
    )
    print(response)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
