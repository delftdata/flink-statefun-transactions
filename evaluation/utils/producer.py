import uuid
import time
import sys

from utils.messages_pb2 import Wrapper, State
from utils.messages_pb2 import Insert
from utils.generator import OperationGenerator
from utils.measure import create_consumer

from google.protobuf.any_pb2 import Any
from confluent_kafka.cimpl import Producer


def insert_records(kafka_broker, record_count):
    producer = create_producer(kafka_broker)

    for i in range(record_count):
        key = str(i + 1)
        fields = create_fields()
        insert = Insert()
        insert.id = key
        insert.state.CopyFrom(
                State(balance=10000, fields=fields))
        wrapped = wrap(str(uuid.uuid4()).replace('-', ''), insert)
        producer.produce('insert', key=key, value=wrapped.SerializeToString())
        if i % 10 == 0:
            producer.flush()
    producer.flush()

    time.sleep(10)
    wait_for_no_new_responses(kafka_broker)
    return


# This code is based on:
# https://github.com/delftdata/flink-test-scripts/blob/master/throughput-producer/Main.py
def produce_records(kafka_broker, record_count, distribution,
                    target_throughput, read, update, transfer):
    print('Starting to produce at throughout: ' + str(target_throughput))
    producer = create_producer(kafka_broker)
    ops_generator = OperationGenerator(record_count, distribution, read,
                                       update, transfer)
    DURATION_IN_MINUTES = 3
    # duration of throughput generation in seconds
    duration_s = DURATION_IN_MINUTES * 60
    # nr of flushes per second
    resolution = 5
    # messages per flush
    messages_produced_per_interval = int(target_throughput / resolution)
    # in case throughput is not divisible by resolution
    # remainder will be produced after
    remainder = target_throughput % resolution

    # milliseconds per flush
    MS_PER_UPDATE = 1000 / resolution

    start_time = current_milli_time()
    last_time = start_time
    current_time = start_time

    lag = 0.0

    produces_this_second = 0
    count = 0
    while current_time < start_time + duration_s * 1000:
        current_time = current_milli_time()
        elapsed = current_time - last_time
        last_time = current_time
        lag += elapsed
        while lag >= MS_PER_UPDATE:
            for i in range(messages_produced_per_interval):
                topic, key, message = ops_generator.next_operation()
                wrapped = wrap(str(uuid.uuid4()).replace('-', ''), message)
                producer.produce(topic, key=key,
                                 value=wrapped.SerializeToString())
                count += 1

            if produces_this_second == resolution-1:
                for i in range(remainder):
                    topic, key, message = ops_generator.next_operation()

                    wrapped = wrap(str(uuid.uuid4()).replace('-', ''), message)
                    producer.produce(topic, key=key,
                                     value=wrapped.SerializeToString())
                    count += 1

            produces_this_second = (produces_this_second + 1) % resolution
            producer.flush()
            lag -= MS_PER_UPDATE
    print("Produced {} messages in {} seconds. Input throughput: {}".format(count, duration_s, count/duration_s))
    print("Target input throughput was {}".format(target_throughput))


def create_producer(kafka_broker):
    return Producer({'bootstrap.servers': kafka_broker})


def wait_for_no_new_responses(kafka_broker):
    res_consumer = create_consumer(kafka_broker, 'responses')
    while True:
        msgs = res_consumer.consume(timeout=5, num_messages=1000)
        if len(msgs) == 0:
            time.sleep(5)
            msgs = res_consumer.consume(timeout=5, num_messages=1000)
            if len(msgs) == 0:
                break


def current_milli_time():
    return int(round(time.time() * 1000))


def wrap(request_id, outgoing):
    wrapped = Wrapper()
    wrapped.request_id = request_id
    message = Any()
    message.Pack(outgoing)
    wrapped.message.CopyFrom(message)
    return wrapped


def create_fields():
    fields = {}
    fields['field0'] = str(uuid.uuid4())
    fields['field1'] = str(uuid.uuid4())
    fields['field2'] = str(uuid.uuid4())
    fields['field3'] = str(uuid.uuid4())
    fields['field4'] = str(uuid.uuid4())
    fields['field5'] = str(uuid.uuid4())
    fields['field6'] = str(uuid.uuid4())
    fields['field7'] = str(uuid.uuid4())
    fields['field8'] = str(uuid.uuid4())
    fields['field9'] = str(uuid.uuid4())
    return fields


if __name__ == "__main__":
    # usage python producer.py produce kafka_broker record_count distribution target_throughput read update transfer
    # OR python producer.py insert kafka_broker record_count
    action = sys.argv[1]
    kafka_broker = sys.argv[2]
    record_count = int(sys.argv[3])
    if action == "produce":
        produce_records(kafka_broker, record_count, sys.argv[4], int(float(sys.argv[5])), int(sys.argv[6]),
                        int(sys.argv[7]), int(sys.argv[8]))
    if action == "insert":
        insert_records(kafka_broker, record_count)
