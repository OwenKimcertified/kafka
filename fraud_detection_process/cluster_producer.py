from kafka import KafkaProducer

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topicname = "first-cluster-topic" 

producer = KafkaProducer(bootstrap_servers = brokers)

producer.send(topicname, b"hello first cluster")
producer.flush() # buffer clear