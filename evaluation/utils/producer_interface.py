import subprocess
import utils.producer as producer


def insert_records(conf, broker, record_count):
    print("Start inserting " + str(record_count) + " records.")
    user = conf['user']
    hosts = conf['producer_hosts']
    if conf['local']:
        producer.insert_records(broker, record_count)
    else:
        cmd = "python producer.py insert {broker} {record_count}".format(broker=broker, record_count=record_count)
        cmd = "docker run martijndeh/producer:fix " + cmd
        p = subprocess.Popen("ssh -o StrictHostKeyChecking=no {user}@{host} {cmd}".format(user=user, host=hosts[0], cmd=cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.communicate()
    print("Done inserting " + str(record_count) + " records")


def produce_records(conf, broker, record_count, distribution,
                    target_throughput, read, update, transfer):
    if conf['local']:
        producer.produce_records(broker, record_count, distribution, target_throughput,
                                 read, update, transfer)
    else:
        hosts = conf['producer_hosts']
        user = conf['user']
        ps = []
        target_throughput_split = target_throughput / len(hosts)
        for i, host in enumerate(hosts):
            print("Starting producer {i} at {t}".format(i=i, t=target_throughput_split))
            cmd = "python producer.py produce {broker} {rc} {dis} {tt} {r} {u} {t}".format(
                    broker=broker, rc=record_count, dis=distribution, tt=target_throughput_split,
                    r=read, u=update, t=transfer)
            cmd = "docker run martijndeh/producer:fix " + cmd
            ps.append(subprocess.Popen("ssh -o StrictHostKeyChecking=no {user}@{host} {cmd}".format(user=user, host=host, cmd=cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE))

        print("Done starting producers!")

        for p in ps:
            print(p.communicate())
    # usage python producer.py produce kafka_broker record_count distribution target_throughput read update transfer
    # OR python producer.py insert kafka_broker record_count
    print("Done producing.")


def pull_producer_images(conf):
    hosts = conf['producer_hosts']
    user = conf['user']
    ps = []
    for i, host in enumerate(hosts):
        cmd = "docker pull martijndeh/producer:fix"
        ps.append(subprocess.Popen("ssh -o StrictHostKeyChecking=no {user}@{host} {cmd}".format(user=user, host=host, cmd=cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE))

    for p in ps:
        print(p.communicate())
    
    print("Done pulling producer images!")