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
    filename='publisher.log', 
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


def validate_type(event):
    for key, values in constants.event_list.items():
        if event["event_type"] in values:
            return key
    return None

'''
def validate_priority(event):
    if event["priority"] in list(constants.priority_list.keys()):
        return event["priority"]
    return None
'''

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting publisher")
    
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
                "event_category": validate_type(event),
                "event_type": event["event_type"],
                "priority": event["priority"], # is this necessary?
                "description": event["description"],
                "timestamp": event["timestamp"]
            }

            '''
            if message['event_category'] is None or message['priority'] is None:
                logging.info(f"Invalid event: {event}")
                continue
            '''
            try:
                producer.send(message["event_type"], key=message["priority"], value=message)
                logging.info(f"Sending event#{i}: {message}\n")
                i += 1
            except Exception as e:
                print(f"Error: {e}")
            finally:
                producer.flush()

if __name__ == "__main__":
    main()
