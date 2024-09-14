import json
import time
import random
import logging
import constants # constants.py
from datetime import datetime
from kafka import KafkaProducer

DATASET = 'datasets/dataset_1.json' 
WAIT_TIME = 1

logging.basicConfig(
    filename='producer.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=20000,  # Timeout after 20 seconds
        retries=5, # Number of retries
        retry_backoff_ms=500,  # Backoff time between retries
    )


def parse_timestamp_to_seconds(timestamp):
    # Convert the timestamp string (MM:SS) to seconds
    time_obj = datetime.strptime(timestamp, '%M:%S')
    return time_obj.minute * 60 + time_obj.second


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting producer")

    producer = create_producer()
    logging.info("Connected to Kafka broker.")
    
    with open(DATASET, 'r') as file:
        events = json.load(file)
    
    events.sort(key=lambda x: parse_timestamp_to_seconds(x['timestamp']))
    
    start_time = time.time()
    
    i = 1
    while events:
        current_time = parse_timestamp_to_seconds(events[0]['timestamp'])
        
        while time.time() - start_time < current_time:
            time.sleep(WAIT_TIME) 
        
        while events and parse_timestamp_to_seconds(events[0]['timestamp']) == current_time:
            event = events.pop(0)
            
            message = {
                "id": event["id"],
                "event_type": event["event_type"],
                "description": event["description"],
                "timestamp": (event["timestamp"], time.time())
            }

            try:
                producer.send(message["event_type"], key=event["priority"], value=message)
                logging.info(f"Sending event #{i}: {message}\n")
                i += 1
            except Exception as e:
                print(f"Error: {e}")
            finally:
                producer.flush()

if __name__ == "__main__":
    main()
