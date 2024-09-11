import logging
import os
import time
import json
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
        retry_backoff_ms=500,  # Backoff time between retries
        value_deserializer=lambda x: x.decode('utf-8')
    )

happy_guests = {}
unhappy_guests = {}


def calculate_expiration(priority):
    if priority == "High":
        return time.time() + 5
    elif priority == "Medium":
        return time.time() + 10
    else:
        return time.time() + 15


def process_message(message):
    deadline = calculate_expiration(message["priority"])
    happy_guests[message["id"]] = ["Happy", deadline, message["event_type"], message["priority"]]
    total_happy = len(happy_guests)
    total_unhappy = len(unhappy_guests)
    logging.info(f"Total={total_happy + total_unhappy}|Happy={total_happy}|Unhappy={total_unhappy}\n")


def happy_to_unhappy(happy_guests):
    for key, value in happy_guests.items():
        if time.time() > value[1]:
            unhappy_guests[key] = [value]
            happy_guests.pop(key, value)


while True:
    for message in consumer:
        try:
            message_dict = json.loads(message.value)
            process_message(message_dict)
            consumer.commit()
            happy_to_unhappy(happy_guests)

        except Exception as e:
            print("ERROR: {e}")

