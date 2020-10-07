# Introduction
This folder contains an example application using Flink Statefun. It uses remote functions implemented using the Python SDK including the a `tpc` (two-phase commit) function.

# Description
The application has 4 stateful functions which can be found in `user`, `stock`, `order` and `checkout` folders. The `checkout` function is a `tpc` function allowing for atomic updates to `user`, `stock` and `order` function instances.

The application has 3 different gateway services which create a simple request/reply layer on top the Kafka igresses and egresses used by the stateful functions. These can be found in the `gateway` folder.

To be able to split the load between multiple instances of the gateways these can scale along with the number of partitions in the Kafka topics. This requires a custom partitioner to route the responses back to the correct gateway instance. This can be found in the `kafka_egress` folder.

# Requirements
* Python
* Docker
* Protobuf (protoc)
* Pytest (for testing)
* Build the Statefun Docker image from this repository to get version including `tpc` functions
* Build the Python Statefun SDK image from this repository to get version including `tpc` functions

# Usage locally
First, the protobuf files can be build using:

`protoc --python_out=protobuf --proto_path=protobuf/proto protobuf/proto/*`

Build the two base images locally by runnning: 

`tools/docker/build-stateful-functions.sh`

And:

`cd statefun-python-sdk`

`docker-compose build`

Then build the project using:

`docker-compose build`

Lastly spin up the services using:

`docker-compose up`

The endpoints are now reachable. The application can be tested by running:

`pytest`