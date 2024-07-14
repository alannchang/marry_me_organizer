import logging
import os
from kafka import KafkaConsumer

logging.basicConfig(
    filename='waiters.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)

kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
        group_id='waiters',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        request_timeout_ms=20000,  # Timeout after 20 seconds
        retry_backoff_ms=500  # Backoff time between retries
    )


def process_message(msg):
        logging.info(f"MESSAGE RECEIVED: {msg}")

while True:
    for message in consumer:
        try:
                process_message(message)
                consumer.commit()
        except Exception as e:
                print("ERROR: {e}")


