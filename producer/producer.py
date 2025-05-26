# import time
# from kafka import KafkaProducer

# TOPIC_NAME = "clickstream"

# producer = KafkaProducer(
#     bootstrap_servers=f"sa-test-assignment-kafka-sa-test-assignment.g.aivencloud.com:16938",
#     security_protocol="SSL",
#     ssl_cafile="../certs/ca.pem",
#     ssl_certfile="../certs/service.cert",
#     ssl_keyfile="../certs/service.key",
# )

# for i in range(100):
#     message = f"Hello from Python using SSL {i + 1}!"
#     producer.send(TOPIC_NAME, message.encode('utf-8'))
#     print(f"Message sent: {message}")
#     time.sleep(1)

# producer.close()
from confluent_kafka import Producer
import json
import time
import random
import socket

conf = {
    'bootstrap.servers': 'sa-test-assignment-kafka-sa-test-assignment.g.aivencloud.com:16938',
    'security.protocol': 'SSL',
    'ssl.ca.location': '../certs/ca.pem',
    'ssl.certificate.location': '../certs/service.cert',
    'ssl.key.location': '../certs/service.key',
    'debug': 'security,broker,protocol'
}

producer = Producer(conf)

def generate_clickstream_event():
    return {
        "user_id": random.randint(1, 10),
        "session_id": random.randint(1000, 9999),
        "event_type": random.choice(["page_view", "click", "scroll"]),
        "event_timestamp": int(time.time()),
        "page_url": random.choice(["/home", "/products", "/about", "/contact"])
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    try:
        while True:
            event = generate_clickstream_event()
            producer.produce("clickstream", json.dumps(event), callback=delivery_report)
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped.")
