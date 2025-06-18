# Multi-schema regular topic to single schema iceberg topics

### Inventory of code items
1. python kafka avro producer - producer_multi_schema_sasl.py
2. redpanda connect pipeline - multi-to-single-schema-topic.yaml

### Steps to run
1. Create a redpanda cluster, either on cloud or local/docker
2. Create raw-topic which supports multiple subject names. Uses topic-record-name subject naming strategy and the producer produces records to it
3. Create individual topics which are iceberg enabled
4. Run the redpanda connect pipeline
5. Produce messages to raw-topic - python producer_multi_schema_sasl.py
6. Redpanda connect pipeline will transform messages from input multischema raw topic to single schema iceberg topics.




