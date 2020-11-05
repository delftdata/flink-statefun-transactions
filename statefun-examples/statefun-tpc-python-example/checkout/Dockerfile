FROM thesis-statefun-python-sdk-base-image:latest

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY checkout/checkout.py /app
COPY protobuf/order_pb2.py /app
COPY protobuf/user_pb2.py /app
COPY protobuf/stock_pb2.py /app

CMD ["gunicorn", "-b", "0.0.0.0:80", "-w 1", "checkout:app"]