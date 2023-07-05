import json

data = {
   "transaction_block_dynamo_table_name":"block_cards",
   "transaction_block_dynamo_table_region":"eu-central-1",
   "risk_txn_1":{
      "txn_block_card_ip_address_number_limit":2,
      "txn_block_card_ip_address_check_hours_range":1
   },
   "risk_txn_2":{
      "txn_block_card_ip_address_number_limit":3,
      "txn_block_card_ip_address_check_hours_range":1
   },
   "risk_txn_4":{
      "txn_block_card_failed_txn_count":5,
      "txn_block_card_ip_failed_txn_interval":1
   },
   "risk_txn_5":{
      "txn_block_card_successful_txn_count":10,
      "txn_block_card_successful_txn_interval_in_minutes":60
   },
   "risk_txn_6":{
      "txn_block_card_successful_txn_count":20,
      "txn_block_card_successful_txn_interval_in_minutes":1440
   },
   "risk_txn_7":{
      "txn_block_card_successful_txn_count":3,
      "txn_block_card_successful_txn_interval_in_minutes":10
   },
   "risk_txn_8":{
      "txn_block_card_ip_address_txn_limit":10,
      "txn_block_card_ip_address_check_hours_range_txn":72
   },
   "risk_txn_9":{
      "txn_block_card_data_check_hours_range":10,
      "txn_block_card_txn_number_limit":5
   },
   "risk_txn_10":{
      "txn_block_card_ip_address_count":2,
      "txn_block_card_merchant_ip_address_interval":24
   }
}

print(data)

# if data.get('risk_txn_0'):
#
#     print(data.get('risk_txn_0').get('data2'))

data1 = None

rule_engine_config = json.loads(data1)

print(type(rule_engine_config))