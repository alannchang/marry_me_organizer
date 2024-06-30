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


def main():
    logging.info("Starting publisher")
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Connected to Kafka broker.")
    logging.info(f"Will generate one unique event every {WAIT_TIME} seconds")

    for i in range(1, EVENT_LIMIT):
        event = random.choice(list(constants.event_list.items()))
        event_category = event[0]
        event_type = random.choice(event[1])
        priority = random.choice(list(constants.priority_list.items()))
        routine = random.choice(list(constants.routine_list.items()))
        data = {
            "event_id": i,
            "event_category": event_category,
            "event_type": event_type,
            "priority": priority, 
            "routine": routine
        }

        producer.send('test_topic', data)
        logging.info(f"Sending event {i}...")
        time.sleep(WAIT_TIME)

if __name__ == "__main__":
    main()
