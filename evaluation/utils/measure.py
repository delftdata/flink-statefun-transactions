from utils.messages_pb2 import Wrapper, Response
import uuid
import os
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)

from confluent_kafka.cimpl import Consumer

def compute_achieved_throughput(broker, partitions_with_offsets, result_dict):
    partitions_with_offsets = {}
    input_consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': str(uuid.uuid4()),
        # 'group.id': 'achieved_throughput_measurer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    output_consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': str(uuid.uuid4()),
        # 'group.id': 'achieved_throughput_measurer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    if 'input' in partitions_with_offsets and len(partitions_with_offsets['input']) > 0:
        input_consumer.assign(partitions_with_offsets['input'])
    else:
        input_consumer.subscribe(['read', 'update', 'transfer'])

    if 'output' in partitions_with_offsets and len(partitions_with_offsets['output']) > 0:
        output_consumer.assign(partitions_with_offsets['output'])
    else:
        output_consumer.subscribe(['responses'])

    while True:
        msgs = input_consumer.consume(timeout=5, num_messages=500)
        if len(msgs) == 0:
            break
        for msg in msgs:
            try:
                wrapped = Wrapper()
                wrapped.ParseFromString(msg.value())

                result = {}
                result['operation'] = msg.topic()
                result['input_time'] = msg.timestamp()[1]
                result_dict[wrapped.request_id] = result
            except DecodeError as e:
                print("Could not decode?")
                pass

    partitions_with_offsets['input'] = input_consumer.position(input_consumer.assignment())
    input_consumer.close()

    total_messages = 0
    start_time = 0
    end_time = 0
    first = True
    
    while True:
        msgs = output_consumer.consume(timeout=5, num_messages=500)
        if len(msgs) == 0:
            break
        for msg in msgs:
            response = Response()
            response.ParseFromString(msg.value())
            key = response.request_id
            status_code = response.status_code
            if key in result_dict:
                if first:
                    start_time = msg.timestamp()[1] / 1000
                    first = False
                total_messages += 1
                end_time = msg.timestamp()[1] / 1000
                result_dict[key]['output_time'] = msg.timestamp()[1]
                result_dict[key]['status_code'] = status_code

    partitions_with_offsets['output'] = output_consumer.position(output_consumer.assignment())
    output_consumer.close()

    print("Total messages considered: " + str(total_messages))

    if total_messages == 0 or end_time - start_time == 0:
        return 0

    return total_messages / (end_time - start_time)


def save_results(result_dict, experiment, operations):
    # result_dict = {}
    # print('Start indexing \'insert\' messages')
    # ic = create_consumer(broker, 'insert')
    # consume_input_topic(ic, 'insert', result_dict)
    # ic.close()

    # print('Start indexing \'read\' messages')
    # ic = create_consumer(broker, 'read')
    # consume_input_topic(ic, 'read', result_dict)
    # ic.close()

    # print('Start indexing \'update\' messages')
    # ic = create_consumer(broker, 'update')
    # consume_input_topic(ic, 'update', result_dict)
    # ic.close()

    # print('Start indexing \'transfer\' messages')
    # ic = create_consumer(broker, 'transfer')
    # consume_input_topic(ic, 'transfer', result_dict)
    # ic.close()

    # print('Start indexing \'response\' messages')
    # oc = create_consumer(broker, 'responses')
    # consume_output_topic(oc, result_dict)
    # oc.close()

    folder_path = '_'.join([experiment['system'], str(experiment['workers']), 
                            str(experiment['record_count']), 'uniform', operations['ops']])
    write_result_dict_to_file(folder_path, result_dict)
    save_plot_to_file(folder_path, result_dict, operations['throughput_steps'] * 3)


def consume_input_topic(consumer, operation, result_dict, resolution=5):
    while True:
        msgs = consumer.consume(timeout=5, num_messages=500)
        if len(msgs) == 0:
            break
        for msg in msgs:
            wrapped = Wrapper()
            wrapped.ParseFromString(msg.value())

            result = {}
            result['operation'] = operation
            result['input_time'] = msg.timestamp()[1]
            result_dict[wrapped.request_id] = result


def consume_output_topic(consumer, result_dict, resolution=5):
    while True:
        msgs = consumer.consume(timeout=5, num_messages=500)
        if len(msgs) == 0:
            break
        for msg in msgs:
            response = Response()
            response.ParseFromString(msg.value())
            key = response.request_id
            status_code = response.status_code
            if key in result_dict:
                result_dict[key]['output_time'] = msg.timestamp()[1]
                result_dict[key]['status_code'] = status_code
            else:
                print('Found output without matching input?')


def create_consumer(kafka_broker, topic):
    ic = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': str(uuid.uuid4()),
        'auto.offset.reset': 'earliest',
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    ic.subscribe([topic])
    return ic


def get_partitions_with_offsets(broker):
    input_consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': str(uuid.uuid4()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    output_consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': str(uuid.uuid4()),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    input_consumer.subscribe(['read', 'update', 'transfer'])
    output_consumer.subscribe(['responses'])

    msgs = input_consumer.consume(timeout=5, num_messages=100)
    if len(msgs) == 0:
        print("returned empty")
        return {}

    partitions_with_offsets = {'input': [], 'output': []}

    input_partitions = input_consumer.assignment()
    for p in input_partitions:
        _, h = input_consumer.get_watermark_offsets(p)
        p.offset = h
        partitions_with_offsets['input'].append(p)
    
    output_consumer.consume(timeout=5, num_messages=100)
    output_partitions = output_consumer.assignment()
    for p in output_partitions:
        _, h = output_consumer.get_watermark_offsets(p)
        p.offset = h
        partitions_with_offsets['output'].append(p)

    return partitions_with_offsets


def write_result_dict_to_file(folder_path, result_dict):
    folder_path = 'results/' + folder_path
    os.makedirs(folder_path, exist_ok=True)
    with open(folder_path + '/all.csv', 'w') as f:
        for key, value in result_dict.items():
            if 'output_time' in value:
                f.write(','.join([value['operation'],
                                  str(value['input_time']),
                                  str(value['output_time']),
                                  str(value['status_code'])]))
                f.write('\n')


def save_plot_to_file(folder_path, result_dict, interval):
    folder_path = 'results/' + folder_path
    os.makedirs(folder_path, exist_ok=True)
    output_times = []
    for key, value in result_dict.items():
        if 'output_time' in value:
            output_times.append(value['output_time'])
    output_times = np.array(output_times) // 1000
    _, counts = np.unique(output_times, return_counts=True)
    fig, ax1 = plt.subplots()
    ax1.set_ylabel('Throughput (req/s)')
    ax1.set_xlabel('Time (s)')
    ax1.plot(counts)
    if interval == 0:
        interval = 500
    ax1.yaxis.set_major_locator(MultipleLocator(interval))
    ax1.yaxis.set_minor_locator(AutoMinorLocator(3))
    ax1.grid(axis='y', which='major', color='#CCCCCC', linestyle='--')
    ax1.grid(axis='y', which='minor', color='#CCCCCC', linestyle=':')
    min_count = min([0] + counts)
    max_count = max([100] + counts)
    ax1.set_ylim(bottom=find_tick(min_count, interval, 'b') , top=find_tick(max_count, interval, 't'))
    plt.savefig(folder_path + '/plot.pdf')


def find_tick(N, K, direction):
    if direction == 't':
        rem = (N + K) % K;  
    
        if (rem == 0):  
            return N 
        else: 
            return (N + K - rem)
    if direction == 'b':
        rem = (N - K) % K;  
    
        if (rem == 0):  
            return N 
        else: 
            return (N - K + (K - rem))