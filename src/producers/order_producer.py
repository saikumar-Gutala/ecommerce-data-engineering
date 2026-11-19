import csv
import json
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'ecommerce_orders'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode('utf-8'),
        acks='all',
        retries=3
    )

def main():
    producer = create_producer()

    with open('data/raw/orders.csv', 'r') as file:
        reader = csv.DictReader(file)

        for order in reader:
            producer.send(TOPIC_NAME, value=order)
            print("Sent order:", order)
            time.sleep(1)

    producer.flush()
    producer.close()
    print("All orders sent successfully!")

if __name__ == '__main__':
    main()
