FROM python:3.7-slim-buster

RUN mkdir -p /app
RUN mkdir -p /app/protobuf
WORKDIR /app

COPY gateway/requirements.txt /app
RUN pip install -r requirements.txt

COPY protobuf/order_pb2.py /app/protobuf
COPY gateway/order/order_gateway.py /app
COPY gateway/kafka_utils.py /app

CMD ["gunicorn", "-b", "0.0.0.0:80", "-w", "1", "--worker-class", \
    "aiohttp.GunicornWebWorker", "order_gateway:app"]