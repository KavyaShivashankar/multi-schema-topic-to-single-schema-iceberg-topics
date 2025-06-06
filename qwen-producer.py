from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, SerializingProducer
from confluent_kafka.serialization import StringSerializer, MessageField


# Configuration for Confluent Kafka with SASL authentication
bootstrap_servers = 'seed-e6b8c8f4.d0v014csdrtl89fnedp0.fmc.prd.cloud.redpanda.com:9092'  # Replace with your broker(s)
schema_registry_url = 'https://schema-registry-c6c68ee0.d0v014csdrtl89fnedp0.fmc.prd.cloud.redpanda.com:30081'  # Replace with your schema registry URL
sasl_mechanism = 'SCRAM-SHA-256'
security_protocol = 'SASLAUTHORIZATION'
sasl_plain_username = 'ks'  # Replace with your username
sasl_plain_password = 'kspwd'  # Replace with your password

# Define the three schemas (replace these with your actual schemas)
schema1 = {
    "type": "record",
    "name": "MyRecord1",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"}
    ]
}

schema2 = {
    "type": "record",
    "name": "MyRecord2",
    "fields": [
        {"name": "value1", "type": "double"},
        {"name": "value2", "type": "float"}
    ]
}

schema3 = {
    "type": "record",
    "name": "MyRecord3",
    "fields": [
        {"name": "message", "type": "string"}
    ]
}

# Register the schemas with the schema registry
schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

# Create a Kafka producer with SASL authentication and the specified configurations
producer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': sasl_mechanism,
    'sasl.username': sasl_plain_username,
    'sasl.password': sasl_plain_password,
    'schema.registry.url': schema_registry_url,
}

# Define the Avro SerDes and create an Avro producer
avro_serializer = lambda record, ctx=None: schema_registry_client.get_latest_version(
    json.dumps(record)).encode(record)

producer = SerializingProducer(producer_conf, value_serializer=avro_serializer)

# Define the topic name
topic_name = 'raw-topic'

# Produce messages with different schemas
records = [
    {"id": 1, "name": "Alice"},
    {"value1": 3.14, "value2": 2.71},
    {"message": "Hello, world!"}
]

for record in records:
    # Get the schema for the current record (you need to implement this logic based on your actual schemas)
    key = json.dumps({"id": list(record.keys())[0]})

    # Send a message with the specified schema
    producer.produce(topic=topic_name, value=record, key=key)

# Wait for all messages to be sent and delivery reports
producer.flush()

print("All records have been produced.")
