from kafka import KafkaConsumer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# KafkaConsumer setup
consumer = KafkaConsumer(
    'user_created',  # Topic name
    bootstrap_servers=['broker:29092'],  # Kafka broker(s)
    auto_offset_reset='earliest',  # Where to start if no committed offset
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='user_consumer_group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)

try:
    logging.info("Starting Kafka Consumer...")
    for message in consumer:
        # Message value
        data = message.value

        # Message metadata
        topic = message.topic
        partition = message.partition
        offset = message.offset

        # Process the message
        logging.info(f"Consumed message from topic {topic}, partition {partition}, offset {offset}: {data}")

except KeyboardInterrupt:
    logging.info("Consumer interrupted by user.")
except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    logging.info("Closing Kafka Consumer...")
    consumer.close()
#20e471600426070d33877f3a15038322ec265acf12e13c51e6641db9e2fa0fb6
#96127d6dacf3c5b62af04e50f9672e34b82e779a4ca015ee00b3b05e3521ff14