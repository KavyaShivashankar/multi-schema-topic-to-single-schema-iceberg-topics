import uuid
import time
import json
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import topic_record_subject_name_strategy 
#from confluent_kafka.schema_registry.avro import TopicRecordNameStrategy
#from io.confluent.kafka.serializers.subject import TopicRecordNameStrategy
# ... your Kafka configuration ...


# --- Configuration ---
# Replace with your Redpanda/Kafka bootstrap servers
BOOTSTRAP_SERVERS = "seed-e6b8c8f4.d0v014csdrtl89fnedp0.fmc.prd.cloud.redpanda.com:9092"
# Replace with your Schema Registry URL
SCHEMA_REGISTRY_URL = "https://schema-registry-c6c68ee0.d0v014csdrtl89fnedp0.fmc.prd.cloud.redpanda.com:30081"
# The topic to which messages will be sent
TOPIC_NAME = "raw-topic"

# --- SASL Authentication Configuration ---
# IMPORTANT: Replace with your actual SASL username and password
# For Redpanda Cloud, this is typically the cluster API key and secret.
SASL_USERNAME = "ks"
SASL_PASSWORD = "ksxxx"
# Use 'SASL_SSL' for encrypted communication, or 'SASL_PLAINTEXT' if SSL is not configured.
# 'SASL_SSL' is highly recommended for production.
SECURITY_PROTOCOL = "SASL_SSL"
SASL_MECHANISM = "SCRAM-SHA-256"


# --- Load Avro Schemas ---
def load_avro_schema_from_file(filepath):
    """Loads an Avro schema from a JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

# Load the three distinct Avro schemas
car_schema_str = json.dumps(load_avro_schema_from_file("schemas/car.avsc"))
van_schema_str = json.dumps(load_avro_schema_from_file("schemas/van.avsc"))
bike_schema_str = json.dumps(load_avro_schema_from_file("schemas/bike.avsc"))

# --- Kafka Callbacks ---
def delivery_report(err, msg):
    """
    Callback function called once for each message produced to indicate delivery success or failure.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' partition [{msg.partition()}] "
              f"at offset {msg.offset()}")

# --- Main Producer Logic ---
def produce_multi_schema_messages():
    """
    Produces messages conforming to different schemas to a single topic
    using TopicRecordNameStrategy with SASL authentication.
    """
    # 1. Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # 2. Initialize Avro Serializers for each schema
    # IMPORTANT: We pass the specific schema string for each serializer.
    # The `TopicRecordNameStrategy` is specified here to tell the serializer
    # how to derive the subject name for schema registration/lookup.
    car_avro_serializer = AvroSerializer(
        schema_registry_client,
        car_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
        #subject_name_strategy=TopicRecordNameStrategy
    )
    van_avro_serializer = AvroSerializer(
        schema_registry_client,
        van_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
        #subject_name_strategy=TopicRecordNameStrategy
    )
    bike_avro_serializer = AvroSerializer(
        schema_registry_client,
        bike_schema_str,
        conf={'subject.name.strategy': topic_record_subject_name_strategy}
        #subject_name_strategy=TopicRecordNameStrategy
    )

    # 3. Initialize Kafka Producer with SASL configuration
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",  # Ensure messages are durably written
        "security.protocol": SECURITY_PROTOCOL,
        "sasl.mechanism": SASL_MECHANISM,
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,
    }
    #producer = Producer(producer_conf)
    producer = Producer(producer_conf)
    try:
        # --- Produce Car Event ---
        car_data = {
            "model": "Model 3",
            "make": "Tesla",
            "year": 2023
        }
        # The SerializationContext tells the serializer the topic and message field (key/value)
        # It's crucial for Subject Name Strategies that incorporate the topic name.
        producer.produce(
            topic=TOPIC_NAME,
            value=car_avro_serializer(
                car_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=car_data["model"].encode('utf-8'), # Key often helps with partitioning
            callback=delivery_report
        )
        print(f"Produced carEvent: {car_data['model']}")
        #time.sleep(1) # Small delay

        # --- Produce Van Event ---
        van_data = {
            "model": "Odyssey",
            "make": "Honda",
            "year": float(2018)
        }
        producer.produce(
            topic=TOPIC_NAME,
            value=van_avro_serializer(
                van_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=van_data["model"].encode('utf-8'),
            callback=delivery_report
        )
        print(f"Produced FileAccessEvent: {van_data['model']}")
        #time.sleep(1)

        # --- Produce Bike Event ---
        bike_data = {
            "model": "Samurai",
            "make": "Suzuki",
            "year": float(2016)
        }
        producer.produce(
            topic=TOPIC_NAME,
            value=bike_avro_serializer(
                bike_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=van_data["model"].encode('utf-8'),
            callback=delivery_report
        )
        print(f"Produced bikeEvent: {bike_data['model']}")
        #time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush(30) # Flush for up to 30 seconds
        print("\nProduction complete.")

if __name__ == "__main__":
    produce_multi_schema_messages()

