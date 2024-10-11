import logging
import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer


class Processor:
    def __init__(self, name):
        self.name = name 
        self.log_filename = f"{name}.log"
        self._setup_logging()
        self.consumer_topics = os.getenv('CONSUMER_TOPICS').split(',')
        self.producer_topics = os.getenv('PRODUCER_TOPICS').split(',')
        self.consumer = self._create_consumer()
        self.producer = self._create_producer()


    def _setup_logging(self):
        logging.basicConfig(
            filename=self.log_filename,
            filemode='w',
            format='%(asctime)s - %(message)s',
            level=logging.INFO
        )


    def _create_consumer(self):
        return KafkaConsumer(
            *self.consumer_topics,
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
            group_id='team',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            request_timeout_ms=20000,
            retry_backoff_ms=500,
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: v.decode('utf-8')
        )


    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
            key_serializer=lambda k: json.dumps(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=20000,  # Timeout after 20 seconds
            retries=5, # Number of retries
            retry_backoff_ms=500,  # Backoff time between retries
        )


    def process_message(self, msg, priority):
        logging.info(f"INCOMING {priority} priority event: {msg}\n")
        self.consumer.commit() 

    def run(self):
        while True:
            for consumed_msg in self.consumer:
                try:
                    message_dict = json.loads(consumed_msg.value)
                    priority = json.loads(consumed_msg.key)
                    self.process_message(message_dict, priority)

                    produced_msg = {
                        "id": message_dict["id"],
                        "event_type": message_dict["event_type"],
                        "priority": priority,
                        "timestamp": message_dict["timestamp"]
                    }
                    
                    self.producer.send(self.name, produced_msg)

                except Exception as e:
                    print(f"ERROR: {e}")
                finally:
                    self.producer.flush()

