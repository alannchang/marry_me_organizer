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
        self.start_time = time.time()
        self.idle_sec, self.work_sec = self.set_routine(routine_type)
        self.time_to_switch = time.time() + self.idle_sec
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
        self.event_dict = {
            "happy": [],
            "stressed": []
        }


    def switch_routine(self):
        self.working = not self.working


    def set_routine(self, routine_type):
        routine = {
            "Standard": (20, 5),
            "Intermittent": (5, 5),
            "Concentrated": (60, 60)
        }
        return routine.get(routine_type)


    def check_time(self):
        current_time = time.time()
        if not self.working:
            if current_time > self.time_to_switch:
                self.working = True
                self.time_to_switch = current_time + self.work_sec
        if self.working:
            if current_time > self.time_to_switch:
                self.working = False
                self.time_to_switch = time.time() + self.idle_sec

        for key, value in self.worker_dict.items():
            if value is not None:
                if key == "High" and current_time > value["timestamp"][1] + 3:
                    self.event_dict["happy"].append(value)
                    self.worker_dict[key] = None
                    self.print_guest_status()
                if key == "Medium"and current_time > value["timestamp"][1] + 3:
                    self.event_dict["happy"].append(value)
                    self.worker_dict[key] = None
                    self.print_guest_status()
                if key == "Low" and current_time > value["timestamp"][1] + 3:
                    self.event_dict["happy"].append(value)
                    self.worker_dict[key] = None
                    self.print_guest_status()


    def work(self, msg):
        current_time = time.time()
        if msg["priority"] == "High" and msg["timestamp"][1] + (5 - 3) > current_time:
            for key, value in self.worker_dict.items():
                if "High" in key and value is None:
                    value = msg
                    logging.info(f"{key} handling: {msg}\n")
                    return
        elif msg["priority"] == "Medium" and msg["timestamp"][1] + (10 - 3) > current_time:
            for key, value in self.worker_dict.items():
                if "Medium" in key and value is None:
                    value = msg
                    logging.info(f"{key} handling: {msg}\n")
                    return
        elif msg["priority"] == "Low" and msg["timestamp"][1] + (15 - 3) > current_time:
            for key, value in self.worker_dict.items():
                if "Low" in key and value is None:
                    value = msg
                    logging.info(f"HANDLING: {msg}\n")
                    return
        self.event_dict["stressed"].append(msg)
        self.print_guest_status()


    def print_guest_status(self):
        logging.info(f"Happy = {len(self.event_dict['happy'])}, Stressed = {len(self.event_dict['stressed'])}")
 

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


    def handle_message(self, msg):
        logging.info(f"RECEIVED: {msg}\n")
        if self.working:
            work(msg)
        else:
            self.event_dict["stressed"].append(msg)
            logging.info(f"STRESSED OUT: {msg}\n")
            self.print_guest_status()


    def run(self):
        while True:
            self.check_time()
            for message in self.consumer:
                try:
                    message_dict = json.loads(message.value)
                    self.handle_message(message_dict)
                    self.consumer.commit()
                except Exception as e:
                    print(f"ERROR: {e}")

