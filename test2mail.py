data = {
   "transaction_block_dynamo_table_name":"block_cards",
   "transaction_block_dynamo_table_region":"eu-central-1",
   "risk_txn_9":{
      "txn_block_card_data_check_hours_range":10,
      "txn_block_card_txn_number_limit":5
   },
   "risk_txn_1":{
      "txn_block_card_ip_address_number_limit":2,
      "txn_block_card_ip_address_check_hours_range":1
   },
   "risk_txn_8":{
      "txn_block_card_ip_address_txn_limit":10,
      "txn_block_card_ip_address_check_hours_range_txn":72
   }
}

print(data.get("risk_txn_1").get("txn_block_card_ip_address_number_limit"))