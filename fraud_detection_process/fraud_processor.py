from kafka import KafkaConsumer 
import json

FRAUD_TOPIC = 'fraud_payments'

brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']

consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers = brokers)

    # schema
    # to_json = {
    #     "Date" : date,
    #     "Time" : t,
    #     "Type" : type,
    #     "Amount" : amount,
    #     "Send" : send,
    # }

for msg in consumer:
    decoded_msg = json.loads(msg.value.decode())
    amount = decoded_msg['Amount']
    send = decoded_msg['Send']
    
    if decoded_msg['Send'] == 'stranger' :
        print(f"[ALERT!!!] FRAUD DETECTED {send} - {amount}")
    
    else:
        print(f"[SUCCESS!!!] {send} - {amount}")