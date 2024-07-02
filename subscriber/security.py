import logging
from kafka import KafkaConsumer

logging.basicConfig(
    filename='security.log', 
    filemode ='w',
    format='%(asctime)s - %(message)s', 
    level=logging.INFO
)

consumer = KafkaConsumer('security', bootstrap_servers=['kafka:9092'])

while True:
    for message in consumer:
        logging.info(f"Msg recd: {message}")
