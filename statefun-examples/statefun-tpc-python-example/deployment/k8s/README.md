# Introduction
This repo shows how to deploy the entire application to kubernetes, specifically minikube for now. 

# Requirements
- Minikube
- Helm

# Usage
First, we start minikube:

`minikube start`

Then, to allow for easy deployment of a Kafka cluster in our kubernetes cluster we should add `strimzi` using Helm:

`helm install strimzi-kafka strimzi/strimzi-kafka-operator`

Now, we can start building the images in the registry of minikube. This is done by first executing:

`eval $(minikube docker-env)`

In the terminal before building the following base images:
- `martijn-thesis-flink-statefun:latest`
- `martijn-thesis-statefun-python-sdk-base-image:latest`

After building these, we can use `dkcb` to build all other images needed for this example (also after executing `eval $(minikube docker-env)`).

Now, we can deploy the entire application with:

`kubectl apply -f .`

While in this folder.

When the application is deployed we can use:

`minikube tunnel`

To reach the endpoints in the gateway from outside of the cluster. Then we can run `pytest` to test the application.