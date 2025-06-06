from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
import avro.schema
import random
import time

# Load schemas
car_schema = avro.schema.parse(open("car.avsc", "r").read())
van_schema = avro.schema.parse(open("van.avsc", "r").read())
bike_schema = avro.schema.parse(open("bike.avsc", "r").read())

# Schema/message options
schemas = [
    ("Car", car_schema, {"model": "Model 3","make": "Tesla","year": float(2023)}),
    ("Van", van_schema, {"model": "Odyssey","make": "Honda","year": float(2018)}),
    ("Bike", bike_schema, {"model": "Samurai","make": "Suzuki","year": float(2016)})
]

# Topic
topic = "raw-topic"

# SASL Auth + Schema Registry + Record naming strategy
producer_config = {
    'bootstrap.servers': 'seed-49a3aaa8.d10ujs3u3l09un9dm200.fmc.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': 'ks',
    'sasl.password': 'kspwd',

    'schema.registry.url': 'https://schema-registry-8584e94d.d10ujs3u3l09un9dm200.fmc.prd.cloud.redpanda.com:30081',
    #'schema.registry.basic.auth.user.info': 'schema_registry_key:schema_registry_secret',

    # Use TopicRecordNameStrategy
    'value.subject.name.strategy': 'io.confluent.kafka.serializers.subject.TopicRecordNameStrategy'
}

# Create the Avro producer
producer = AvroProducer(producer_config)

try:
    for i in range(10):
        name, schema, data = random.choice(schemas)
        print(f"[{i}] Producing {name}: {data}")
        producer.produce(topic=topic, value=data, value_schema=schema)
        producer.flush()
        time.sleep(1)
except SerializerError as e:
    print("Serialization error:", e)
finally:
    producer.flush()
