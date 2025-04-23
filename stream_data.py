
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import requests

default_args = {
    'owner':'airflow',
    'start_date': pendulum.datetime(2024,12,13)
}
def stream_data():

    # Kafka Configuration
    KAFKA_BROKER = "localhost:29092"   # Replace with your Kafka broker address
    KAFKA_TOPIC = "user_created"   # Replace with your Kafka topic name

    # MongoDB Configuration
    MONGO_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
    MONGO_DATABASE = "stream"          # Replace with your database name
    MONGO_COLLECTION = "data"      # Replace with your collection name

    # Create Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',  # Start reading from the beginning
        enable_auto_commit=True,       # Commit offsets automatically
        group_id="your_consumer_group",  # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
    )

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017')
    db = client['stream']
    collection = db['data']

    print(f"Listening to Kafka topic '{KAFKA_TOPIC}' and saving to MongoDB...")

    # Consume messages and save to MongoDB
    try:
        for message in consumer:
            # Message value
            data = message.value
            print(f"Received message: {data}")

            # Insert into MongoDB
            collection.insert_one(data)
            print("Message inserted into MongoDB.")

    except KeyboardInterrupt:
        print("Consumer interrupted by user.")

    finally:
        # Close connections
        consumer.close()
        client.close()
        print("Kafka consumer and MongoDB connection closed.")

with DAG('stream_data',
         default_args=default_args,
         schedule= '@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
         task_id = 'stream_data',
         python_callable=stream_data
    )
