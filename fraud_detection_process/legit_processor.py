from kafka import KafkaConsumer 
import json

LEGIT_TOPIC = 'legit_payments'

brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']

consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers = brokers)

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
    # change to python objects
    amount = decoded_msg['Amount']
    send = decoded_msg['Send']
   
    if decoded_msg['Type'] == 'Visa':
        print(f"[Visa] {send} - {amount}")

    elif decoded_msg['Type'] == 'Master':
        print(f"[Master] {send} - {amount}")

    else:
        print(f"[ALERT!!!] UNABLE TYPE {send} - {amount}")
