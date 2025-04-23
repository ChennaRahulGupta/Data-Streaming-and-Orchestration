
from kafka import KafkaConsumer
from pymongo import MongoClient
import json




def stream_data():

    # Kafka Configuration
    KAFKA_BROKER = "localhost:9092"   # Replace with your Kafka broker address
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
        #group_id="_confluent-controlcenter-7-4-0-1",  # Consumer group ID
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

stream_data()

