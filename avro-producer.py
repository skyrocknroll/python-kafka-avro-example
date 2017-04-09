from time import sleep

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('schema/producer/ValueSchema.avsc')
key_schema = avro.load('schema/producer/KeySchema.avsc')
key = {"name": "Rathi"}
value = {"name": "Value", "favorite_number": 10, "favorite_color": "green", "age": 25}

avroProducer = AvroProducer(
    {'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://127.0.0.1:8081'},
    default_key_schema=key_schema, default_value_schema=value_schema)

for i in range(0, 100000):
    value = {"name": "Yuva", "favorite_number": 10, "favorite_color": "green", "age": i}
    avroProducer.produce(topic='my_topic', value=value, key=key, key_schema=key_schema, value_schema=value_schema)
    sleep(0.01)
    print(i)

avroProducer.flush(10)
