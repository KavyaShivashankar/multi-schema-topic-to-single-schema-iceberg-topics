import uuid
import time
import json
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import TopicRecordNameStrategy

# --- Configuration ---
# Replace with your Redpanda/Kafka bootstrap servers
BOOTSTRAP_SERVERS = "seed-49a3aaa8.d10ujs3u3l09un9dm200.fmc.prd.cloud.redpanda.com:9092"
# Replace with your Schema Registry URL
#SCHEMA_REGISTRY_URL = "http://localhost:8081"
SCHEMA_REGISTRY_URL="https://schema-registry-8584e94d.d10ujs3u3l09un9dm200.fmc.prd.cloud.redpanda.com:30081"
# The topic to which messages will be sent
TOPIC_NAME = "raw-topic"

# --- Load Avro Schemas ---
def load_avro_schema_from_file(filepath):
    """Loads an Avro schema from a JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

# Load the three distinct Avro schemas
user_login_schema_str = json.dumps(load_avro_schema_from_file("user_login.avsc"))
file_access_schema_str = json.dumps(load_avro_schema_from_file("file_access.avsc"))
config_change_schema_str = json.dumps(load_avro_schema_from_file("config_change.avsc"))

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
    using TopicRecordNameStrategy.
    """
    # 1. Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # 2. Initialize Avro Serializers for each schema
    # IMPORTANT: We pass the specific schema string for each serializer.
    # The `TopicRecordNameStrategy` is specified here to tell the serializer
    # how to derive the subject name for schema registration/lookup.
    user_login_avro_serializer = AvroSerializer(
        schema_registry_client,
        user_login_schema_str,
        subject_name_strategy=TopicRecordNameStrategy()
    )
    file_access_avro_serializer = AvroSerializer(
        schema_registry_client,
        file_access_schema_str,
        subject_name_strategy=TopicRecordNameStrategy()
    )
    config_change_avro_serializer = AvroSerializer(
        schema_registry_client,
        config_change_schema_str,
        subject_name_strategy=TopicRecordNameStrategy()
    )

    # 3. Initialize Kafka Producer
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",  # Ensure messages are durably written
    }
    producer = Producer(producer_conf)

    try:
        # --- Produce User Login Event ---
        user_login_data = {
            "eventId": str(uuid.uuid4()),
            "timestamp": int(datetime.now().timestamp() * 1000),
            "userId": "user_alice",
            "username": "Alice Smith",
            "ipAddress": "192.168.1.100",
            "loginSuccess": True,
            "authMethod": "PASSWORD"
        }
        # The SerializationContext tells the serializer the topic and message field (key/value)
        # It's crucial for Subject Name Strategies that incorporate the topic name.
        producer.produce(
            topic=TOPIC_NAME,
            value=user_login_avro_serializer(
                user_login_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=user_login_data["userId"].encode('utf-8'), # Key often helps with partitioning
            callback=delivery_report
        )
        print(f"Produced UserLoginEvent: {user_login_data['eventId']}")
        time.sleep(1) # Small delay

        # --- Produce File Access Event ---
        file_access_data = {
            "eventId": str(uuid.uuid4()),
            "timestamp": int(datetime.now().timestamp() * 1000),
            "userId": "user_bob",
            "filePath": "/docs/sensitive/report_q2.pdf",
            "accessType": "READ",
            "bytesTransferred": 1024 * 500 # 500 KB
        }
        producer.produce(
            topic=TOPIC_NAME,
            value=file_access_avro_serializer(
                file_access_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=file_access_data["userId"].encode('utf-8'),
            callback=delivery_report
        )
        print(f"Produced FileAccessEvent: {file_access_data['eventId']}")
        time.sleep(1)

        # --- Produce Config Change Event ---
        config_change_data = {
            "eventId": str(uuid.uuid4()),
            "timestamp": int(datetime.now().timestamp() * 1000),
            "userId": "admin_clark",
            "configKey": "max_connections",
            "oldValue": "100",
            "newValue": "250"
        }
        producer.produce(
            topic=TOPIC_NAME,
            value=config_change_avro_serializer(
                config_change_data,
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            ),
            key=config_change_data["userId"].encode('utf-8'),
            callback=delivery_report
        )
        print(f"Produced ConfigChangeEvent: {config_change_data['eventId']}")
        time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush(30) # Flush for up to 30 seconds
        print("\nProduction complete.")

if __name__ == "__main__":
    produce_multi_schema_messages()

