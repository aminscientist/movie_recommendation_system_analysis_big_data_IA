from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer
import requests
import json
import time

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': "localhost:9092"
}

# Kafka topics
users_topic = 'users_topic'
movies_topic = 'movies_topic'
ratings_topic = 'ratings_topic'

# Flask API URLs
users_api_url = 'http://localhost:5000/api/users'
movies_api_url = 'http://localhost:5000/api/movies'
ratings_api_url = 'http://localhost:5000/api/ratings'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        print('Message content: {}'.format(msg.value().decode('utf-8')))

def produce_data_with_delay(api_url, kafka_producer, kafka_topic):
    try:
        # Fetch data from the API
        response = requests.get(api_url)
        data = response.json()

        # Produce data to Kafka topic with a delay
        for record in data:
            record_value = json.dumps(record)
            kafka_producer.produce(kafka_topic, key=None, value=record_value, callback=delivery_report)
            kafka_producer.flush()  # Flush after each message to ensure immediate delivery
            time.sleep(1)

    except Exception as e:
        print(f"Error fetching or producing data: {e}")

if __name__ == '__main__':
    # Create Kafka producer instance
    producer = Producer(kafka_config)

    # Use ThreadPoolExecutor to parallelize data transfer
    with ThreadPoolExecutor() as executor:
        # Schedule the execution of produce_data_with_delay for each API
        futures = [
            executor.submit(produce_data_with_delay, users_api_url, producer, users_topic),
            executor.submit(produce_data_with_delay, movies_api_url, producer, movies_topic),
            executor.submit(produce_data_with_delay, ratings_api_url, producer, ratings_topic)
        ]

        # Wait for all futures to complete
        for future in futures:
            future.result()

    # Sleep for a while to allow the producer to finish delivering messages
    time.sleep(5)