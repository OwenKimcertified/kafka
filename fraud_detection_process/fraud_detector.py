from kafka import KafkaProducer, KafkaConsumer
import json

PAYMENTS_TOPIC = 'payments'
FRAUD_TOPIC = 'fraud_payments'
LEGIT_TOPIC = 'legit_payments'

brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']

consumer = KafkaConsumer(PAYMENTS_TOPIC, bootstrap_servers = brokers)
producer = KafkaProducer(bootstrap_servers = brokers)

def suspicious(tx):
    """tx = transaction"""
    if tx['Type'] == 'black' or tx['Send'] == 'stranger':
        return True
    else:
        return False


for msg in consumer:
    decoded_msg = json.loads(msg.value.decode())
    classify_topic = FRAUD_TOPIC if suspicious(decoded_msg) else LEGIT_TOPIC
    producer.send(classify_topic, json.dumps(decoded_msg).encode('utf-8'))

    print(classify_topic, suspicious(decoded_msg), decoded_msg['Type'])