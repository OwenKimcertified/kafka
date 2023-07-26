from kafka import KafkaProducer
import datetime, time, pytz, random, json
# pytz -> timeson,  
# check module name and variable


brokers = ['localhost:9091', 'localhost:9092', 'localhost:9093']
producer = KafkaProducer(bootstrap_servers = brokers)

topicname = 'payments'

def get_date_time():
    utc = pytz.utc.localize(datetime.datetime.utcnow())
    kst = utc.astimezone(pytz.timezone("Asia/Seoul"))
    date = kst.strftime("%m/%d/%Y")
    t = kst.strftime("%H:%M:%S")
    return date, t

def payment_generator():
    type = random.choice(['VISA', 'MASTER','black'])
    amount = random.randint(0, 100)
    send = random.choice(['a', 'b', 'c', 'd', 'stranger'])
    return type, amount, send

while True:
    date, t = get_date_time()
    type, amount, send = payment_generator()
    
    to_json = {
        "Date" : date,
        "Time" : t,
        "Type" : type,
        "Amount" : amount,
        "Send" : send,
    }

    producer.send(topicname, json.dumps(to_json).encode('utf-8'))
    print(to_json)
    time.sleep(1)