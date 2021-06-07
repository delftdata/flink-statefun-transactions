from utils.kubernetes import refresh_kubernetes_cluster, refresh_topics
import utils.producer_interface as producer_interface
from utils.measure import compute_achieved_throughput, save_results, get_partitions_with_offsets
import time
import json


def run(conf, experiment, broker, record_count, distribution, operations):
    read, update, transfer = split_operations(operations['ops'])
    found_throughput_limit = False
    target_throughput = operations['initial_throughput']
    result_dict = {}
    i = 1
    while not found_throughput_limit:
        refresh_topics()
        producer_interface.insert_records(conf, broker, experiment['record_count'])
        time.sleep(10)
        partitions_with_offsets = get_partitions_with_offsets(broker)
        print("Start producing at: " + str(target_throughput))
        producer_interface.produce_records(conf, broker, record_count, distribution,
                        target_throughput, read, update, transfer)
        print("Start waiting for messages to process.")
        time.sleep(10)
        print("Done waiting for messages to process.")
        throughput = compute_achieved_throughput(broker, partitions_with_offsets, result_dict)
        print("================================ For experiment ================================")
        print(experiment)
        print(operations['ops'])
        print('Achieved throughput: {at} of target throughput: {tt}'.format(at=throughput, tt=target_throughput))
        if throughput > 0.95 * target_throughput and operations['throughput_steps'] > 0:
            target_throughput += operations['throughput_steps']
        else:
            found_throughput_limit = True
        i += 1
    return result_dict


def main(conf):
    if not conf['local']:
        producer_interface.pull_producer_images(conf)
    for experiment in conf['experiments']:
        print("Starting experiment:")
        print(str(experiment))
        for operations in experiment['operations']:
            kafka_port = refresh_kubernetes_cluster(conf, experiment['system'], experiment['workers'])
            broker = ""
            if conf['local']:
                broker = "127.0.0.1:9094"
            else:
                broker = conf['machine_ip'] + ":" + kafka_port
            # producer_interface.insert_records(conf, broker, experiment['record_count'])
            # time.sleep(30)
            print("Starting operations: " + operations['ops'])
            result_dict = run(conf, experiment, broker, experiment['record_count'], conf['distribution'], operations)
            save_results(result_dict, experiment, operations)


def split_operations(operations):
    split = operations.split('_')
    read = int(split[0])
    update = int(split[2])
    if len(split) > 4:
        transfer = int(split[4])
    else:
        transfer = 0
    return read, update, transfer


if __name__ == "__main__":
    with open("config.json") as json_conf_file:
        conf = json.load(json_conf_file)

    main(conf)
