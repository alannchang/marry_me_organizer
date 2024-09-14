import logging
import os
import json
import time
from kafka import KafkaConsumer


class Worker:
    def __init__(self, name, routine_type):
        self.name = name
        # logging
        self.log_filename = f"{name}.log"
        self._setup_logging()
        # consuming
        self.consumer_topics = os.getenv('CONSUMER_TOPICS').split(',')
        self.consumer = self._create_consumer()
        # handling
        # self.idle_time, self.work_time = set_routine(routine_type)
        self.working = False
        self.worker_dict = {
          "High-1": None,
          "High-2": None,
          "High-3": None,
          "High-4": None,
          "Medium-1": None,
          "Medium-2": None,
          "Low": None
        }


    def start_work(self):
        self.working = True
        self._run_routine()


    def _run_routine(self):
        while self.working:
            self.go_idle()
            time.sleep(self.idle_time)
            self.go_work()
            time.sleep(self.work_time)


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


    def handle_event(self, msg):
        logging.info(f"INCOMING event: {msg}")
                

    def run(self):
        while True:
            for message in self.consumer:
                try:
                    message_dict = json.loads(message.value)
                    self.handle_event(message_dict)
                    self.consumer.commit()
                except Exception as e:
                    print(f"ERROR: {e}")

