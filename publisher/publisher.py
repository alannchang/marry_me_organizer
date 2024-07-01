import json
import time
import random
import logging
import constants # constants.py

from kafka import KafkaProducer#, KafkaAdminClient

EVENT_LIMIT = 15
WAIT_TIME = 10

logging.basicConfig(
    filename='publisher.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)


def random_event_generator():
    '''
    Event format example

    event = {
        event_type: feeling_ill,
        priority: medium,
        description: guest has stomach ache after eating 5 pieces of cake.
    }
    '''
    event = random.choice(list(constants.event_list.items()))
    event_type = random.choice(event[1])
    priority = random.choice(list(constants.priority_list.keys()))
    event = {
        "event_type": event_type,
        "priority": priority, 
        "description": "what's the description for?"
    }
    return event


def validate_type(event):
    for key, values in constants.event_list.items():
        if event["event_type"] in values:
            return key
    return None

def validate_priority(event):
    if event["priority"] in list(constants.priority_list.keys()):
        return event["priority"]
    return None


def main():
    logging.info("Starting publisher")
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Connected to Kafka broker.")
    logging.info(f"Will generate one unique event every {WAIT_TIME} seconds")
    
    for i in range(1, EVENT_LIMIT):
        event = random_event_generator()
        event_category = validate_type(event) 
        priority = validate_priority(event)
        if event_category == None or priority == None:
            logging.info(f"Invalid event: {event}")
            print(f"Invalid event: {event}")
            continue
        message = {
            "event_id": i,
            "event_category": event_category,
            "event_type": event["event_type"],
            "priority": event["priority"],
            "description": event["description"]
        }

        producer.send(f"event_category", message)
        logging.info(f"Sending event {i}: {message}" )
        time.sleep(WAIT_TIME)

if __name__ == "__main__":
    main()
