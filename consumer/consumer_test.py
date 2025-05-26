import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

TOPIC_NAME = "clickstream"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def main():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers="sa-test-assignment-kafka-sa-test-assignment.g.aivencloud.com:16938",
            security_protocol="SSL",
            ssl_cafile="../certs/ca.pem",
            ssl_certfile="../certs/service.cert",
            ssl_keyfile="../certs/service.key",
            auto_offset_reset='earliest',
            group_id="my-python-consumer-group",
            consumer_timeout_ms=10000  # exit if no messages for 10 seconds
        )
        logging.info(f"Connected to Kafka, listening on topic '{TOPIC_NAME}'...")

        for message in consumer:
            logging.info(f"Received message: {message.value.decode('utf-8')}")

        logging.info("No new messages, closing consumer.")

    except KafkaError as e:
        logging.error(f"Kafka error: {e}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
