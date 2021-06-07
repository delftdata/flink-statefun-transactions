import time
import sys
import subprocess
import re


def refresh_topics():
    subprocess.run("kubectl delete -f ./k8s/topics.yaml > /dev/null", shell=True, capture_output=True)
    time.sleep(10)
    subprocess.run("kubectl apply -f ./k8s/topics.yaml > /dev/null", shell=True, capture_output=True)
    kafka_ready = False
    topics = ['insert']
    while not kafka_ready:
        res = subprocess.run(
                'kubectl exec kafka-cluster-kafka-0 -- bin/kafka-topics.sh --list --bootstrap-server localhost:9092',
                shell=True,
                capture_output=True)
        topics_ready = all(topic in res.stdout.decode('utf-8') for topic in topics)
        if topics_ready:
            print("Kafka topics ready -- continuing")
            kafka_ready = True
        else:
            print("Kafka topics not yet ready")
            time.sleep(10)


def refresh_kubernetes_cluster(conf, system, nr_of_workers):
    print('Start cleaning up cluster!')
    cleanup_cluster()
    if conf['local']:
        system = system + "-local"
    print('Done cleaning up cluster! Start deploying!')
    deploy(system, nr_of_workers)
    print('Done deploying!')
    return get_kafka_broker_port()


def get_kafka_broker_port():
    port = subprocess.run("kubectl get service kafka-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{\"\\n\"}'", shell=True, capture_output=True)
    port = port.stdout.decode('utf-8').strip()
    return port


def cleanup_cluster():
    subprocess.run("kubectl delete -f ./k8s/tpc/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/sagas/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/original/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/tpc-local/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/sagas-local/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/original-local/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl delete -f ./k8s/internal-invocation/. > /dev/null", shell=True, capture_output=True)
    subprocess.run("kubectl wait --for=delete pods/kafka-cluster-zookeeper-0 --timeout=180s", shell=True, capture_output=True)
    subprocess.run("kubectl wait --for=delete pods/kafka-cluster-kafka-0 --timeout=180s", shell=True, capture_output=True)


def deploy(system, nr):
    update_flink_workers_yaml(nr, './k8s/' + system + '/flink-statefun.yaml')
    subprocess.run("kubectl apply -f ./k8s/" + system + "/. > /dev/null", shell=True)
    time.sleep(60)
    subprocess.run("kubectl wait --for=condition=Ready pods/kafka-cluster-zookeeper-0 --timeout=180s", shell=True)
    time.sleep(30)
    subprocess.run("kubectl wait --for=condition=Ready pods/kafka-cluster-kafka-0 --timeout=180s", shell=True)
    time.sleep(10)
    subprocess.run("kubectl wait --for=condition=Available deployments/statefun-worker --timeout=180s", shell=True)
    kafka_ready = False
    # topics = ['insert', 'read', 'update']
    topics = ['insert']
    while not kafka_ready:
        res = subprocess.run(
                'kubectl exec kafka-cluster-kafka-0 -- bin/kafka-topics.sh --list --bootstrap-server localhost:9092',
                shell=True,
                capture_output=True)
        topics_ready = all(topic in res.stdout.decode('utf-8') for topic in topics)
        if topics_ready:
            print("Kafka topics ready -- continuing")
            kafka_ready = True
        else:
            print("Kafka topics not yet ready")
            time.sleep(20)
    statefun_ready = False
    while not statefun_ready:
        res = subprocess.run(
                'kubectl exec deploy/statefun-worker -- bin/flink list -r',
                shell=True,
                capture_output=True)
        if "StatefulFunctions (RUNNING)" in res.stdout.decode('utf-8'):
            print("Statefun ready -- continuing")
            statefun_ready = True
        else:
            print("Statefun not yet ready")
            time.sleep(5)
    return


def update_flink_workers_yaml(nr, filepath):
    with open(filepath, 'r') as f:
        filedata = f.read()

    filedata = re.sub(r'parallelism.default: [0-9]+',
                      'parallelism.default: ' + str(nr),
                      filedata)
    filedata = re.sub(r'worker\nspec:\n  replicas: [0-9]+',
                      'worker\nspec:\n  replicas: ' + str(nr),
                      filedata)

    with open(filepath, 'w') as f:
        f.write(filedata)
    return
