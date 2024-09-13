import logging
import os
import json
from kafka import KafkaConsumer

logging.basicConfig(
    filename='security.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)

kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
        group_id='team',
        auto_offset_reset='earliest',  # Start reading at the earliest offset if no previous offset is found
        enable_auto_commit=False,  # Disable auto-commit of offsets
        request_timeout_ms=20000,  # Timeout after 20 seconds
        retry_backoff_ms=500,  # Backoff time between retries
        value_deserializer=lambda x:x.decode('utf-8')
    )


def process_message(msg, priority):
    logging.info(f"{priority} event: {msg}")


worker_dict = {
    "High-1": None,
    "High-2": None,
    "High-3": None,
    "High-4": None,
    "Medium-1": None,
    "Medium-2": None,
    "Low" : None
}

while True:
    for message in consumer:
        try:
            message_dict = json.loads(message.value)
            priority = json.loads(message.key)
            process_message(message_dict, priority)
            consumer.commit()
        except Exception as e:
            print("ERROR: {e}")


