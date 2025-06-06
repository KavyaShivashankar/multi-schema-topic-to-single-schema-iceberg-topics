#https://stackoverflow.com/questions/72027382/how-to-change-subjectnamestrategy-and-use-different-schemas-in-confluent-kafka-p
from confluent_kafka.schema_registry import  topic_record_subject_name_strategy
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.avro import SerializerError
import avro.schema
from faker import Faker

fake = Faker()

# Define multiple Avro schemas for the values
value_schema_str_1 = """
{
   "namespace": "my.test",
   "name": "value1",
   "type": "record",
   "fields" : [
     {
       "name" : "language",
       "type" : "string"
     },
     {
       "name" : "greeting",
       "type" : "string"
     }
   ]
}
"""

value_schema_str_2 = """
{
   "namespace": "my.test",
   "name": "value2",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     },
     {
       "name" : "age",
       "type" : "int"
     }
   ]
}
"""

# Define the Avro schema for the key
key_schema_str = """
{
   "namespace": "my.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "timestamp",
       "type" : "long"
     }
   ]
}
"""

# Parse the schema strings into AvroSerializer
value_schema_1 = avro.schema.parse(value_schema_str_1)
value_schema_2 = avro.schema.parse(value_schema_str_2)
key_schema = avro.schema.parse(key_schema_str)

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'http://localhost:8081'
})

# Create AvroSerializers using the parsed schemas and the Schema Registry client
value_serializer_1 = AvroSerializer(schema_str=value_schema_str_1,
                                    schema_registry_client=schema_registry_client,
                                    conf={'subject.name.strategy': topic_record_subject_name_strategy})
value_serializer_2 = AvroSerializer(schema_str=value_schema_str_2,
                                    schema_registry_client=schema_registry_client,
                                    conf={'subject.name.strategy': topic_record_subject_name_strategy})
key_serializer = AvroSerializer(schema_str=key_schema_str,
                                schema_registry_client=schema_registry_client)

# Function to create and send a message with a specific value serializer
def send_message(value_serializer, value):
    try:
      # SerializingProducer configuration
      producer_config = {
          'bootstrap.servers': 'localhost:9092',
          'key.serializer': key_serializer,
          'value.serializer': value_serializer
      }

      # Create a SerializingProducer instance with the configuration
      serializing_producer = SerializingProducer(producer_config)
      key = {"timestamp": int(fake.unix_time())}
      serializing_producer.produce(topic='example-stream-avro', value=value, key=key)
      serializing_producer.flush()
      print(f"Message sent successfully")
    except SerializerError as e:
        print(f"Message serialization failed: {str(e)}")
    except Exception as e:
        print(f"Message production failed: {str(e)}")

# Send a message using the first value schema
value1 = {"language": "ENGLISH", "greeting": "Hello, World!"}
send_message(value_serializer_1, value1)

# Send a message using the second value schema
name = fake.name()
value2 = {"name": name, "age": 30}
send_message(value_serializer_2, value2)