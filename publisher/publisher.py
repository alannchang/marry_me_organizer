import json
import time
import random
import constants # constants.py

# kafka-python
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable

EVENT_LIMIT = 15
WAIT_TIME = 10

def wait_for_kafka():
    while True:
        try:
            print("Attempting to connect to Kafka...")
            admin_client = KafkaAdminClient(bootstrap_servers=['kafka:9092'])
            admin_client.list_topics()
            break
        except NoBrokersAvailable:
            print("Kafka broker not available, waiting...")
            time.sleep(5)

def main():
    print("Starting publisher")
    wait_for_kafka()
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Connected to Kafka broker.")
    print(f"Will generate one unique event every {WAIT_TIME} seconds")

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
        print(f"Sending event {i}...")
        time.sleep(WAIT_TIME)

if __name__ == "__main__":
    main()
