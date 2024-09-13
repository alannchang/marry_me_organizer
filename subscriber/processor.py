import logging
import os
import json
from kafka import KafkaConsumer

class Processor:
    def __init__(self, log_filename):
        self.log_filename = log_filename
        self._setup_logging()
        self.kafka_topics = os.getenv('KAFKA_TOPICS').split(',')
        self.consumer = self._create_consumer()

    def _setup_logging(self):
        logging.basicConfig(
            filename=self.log_filename,
            filemode='w',
            format='%(asctime)s - %(message)s',
            level=logging.INFO
        )

    def _create_consumer(self):
        return KafkaConsumer(
            *self.kafka_topics,
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
            group_id='team',
            auto_offset_reset='earliest',  # Start reading at the earliest offset if no previous offset is found
            enable_auto_commit=False,  # Disable auto-commit of offsets
            request_timeout_ms=20000,  # Timeout after 20 seconds
            retry_backoff_ms=500,  # Backoff time between retries
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: v.decode('utf-8')
        )

    def process_message(self, msg, priority):
        logging.info(f"{priority} priority event: {msg}")

    def run(self):
        while True:
            for message in self.consumer:
                try:
                    message_dict = json.loads(message.value)
                    priority = json.loads(message.key)
                    self.process_message(message_dict, priority)
                    self.consumer.commit()
                except Exception as e:
                    print(f"ERROR: {e}")

