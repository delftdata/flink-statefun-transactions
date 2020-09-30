FROM python:3.7-slim-buster

RUN mkdir -p /app
RUN mkdir -p /app/protobuf
WORKDIR /app

COPY gateway/requirements.txt /app
RUN pip install -r requirements.txt

COPY protobuf/user_pb2.py /app/protobuf
COPY gateway/user/user_gateway.py /app
COPY gateway/kafka_utils.py /app

CMD ["gunicorn", "-b", "0.0.0.0:8000", "-w", "1", "--worker-class", \
    "aiohttp.GunicornWebWorker", "user_gateway:app"]