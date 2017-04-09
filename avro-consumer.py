from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.cimpl import TopicPartition

c = AvroConsumer(
    {'bootstrap.servers': 'localhost:9092', 'group.id': 'cgroudid-2', 'schema.registry.url': 'http://127.0.0.1:8081',
     "api.version.request": True})
c.subscribe(['my_topic'])
running = True
while running:
    msg = None
    try:
        msg = c.poll(10)
        if msg:
            if not msg.error():
                print(msg.value())
                print(msg.key())
                print(msg.partition())
                print(msg.offset())
                c.commit(msg)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
        else:
            print("No Message!! Happily trying again!!")
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False
c.commit()
c.close()
