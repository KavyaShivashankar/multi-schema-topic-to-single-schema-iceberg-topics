import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
  bootstrap_servers="seed-e6b8c8f4.d0v014csdrtl89fnedp0.fmc.prd.cloud.redpanda.com:9092",
  security_protocol="SASL_SSL",
  sasl_mechanism="SCRAM-SHA-256",
  sasl_plain_username="ks",
  sasl_plain_password="kspwd",
)
hostname = str.encode(socket.gethostname())

def on_success(metadata):
  print(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

# Produce 100 messages asynchronously
for i in range(100):
  msg = f"asynchronous message #{i}"
  future = producer.send(
    "raw-topic",
    key=hostname,
    value=str.encode(msg)
  )
  future.add_callback(on_success)
  future.add_errback(on_error)
producer.flush()
producer.close()
