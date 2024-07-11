import logging
import os
from kafka import KafkaConsumer

logging.basicConfig(
    filename='clean_up.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)

kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
        group_id='clean_up',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        request_timeout_ms=20000,  # Timeout after 20 seconds
        retry_backoff_ms=500  # Backoff time between retries
    )

while True:
    for message in consumer:
        logging.info(f"Msg recd: {message}")
