FROM martijn-thesis-statefun-python-sdk-base-image:latest

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY stock/stock.py /app
COPY protobuf/stock_pb2.py /app

CMD ["gunicorn", "-b", "0.0.0.0:81", "-w 1", "stock:app"]