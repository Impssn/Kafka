from kafka import KafkaProducer
import json
import time
import random

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_messages():
    for _ in range(10):
        message = {'number': random.randint(0, 100)}
        print(f'Sending: {message}')
        producer.send('test', message)
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    send_messages()
