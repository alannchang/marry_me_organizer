import logging
import os
import time
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(
    filename='guests.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)

kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
        group_id='guests',
        auto_offset_reset='earliest',  # Start reading at the earliest offset if no previous offset is found
        enable_auto_commit=False,  # Disable auto-commit of offsets
        request_timeout_ms=20000,  # Timeout after 20 seconds
        retry_backoff_ms=500  # Backoff time between retries
    )


guest_db = {}


def calculate_expiration(priority):
    if priority == "High":
        return time.time() + 5
    elif priority == "Medium":
        return time.time() + 10
    else:
        return time.time() + 15


def process_message(message):
    deadline = calculate_expiration(message["priority"])
    guest_db[message["id"]] = ["Happy", deadline, message["event_type"]]
    logging.info(f"{datetime.datetime.now()}: {guest_db}")


while True:
    for message in consumer:
        try:
                process_message(message)
                consumer.commit()
        except Exception as e:
                print("ERROR: {e}")

