import uuid
import time
import random
import os

import asyncio

from aiohttp import web

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.errors import KafkaConnectionError, NoBrokersAvailable

KAFKA_BROKER = os.environ['KAFKA_BROKER']


async def create_consumer(app: web.Application):
    logger = app['logger']
    logger.info('Starting Kafka consumer...')

    broker_available = False

    while not broker_available:
        try:
            consumer = AIOKafkaConsumer(
                group_id=app['group_id'],
                loop=asyncio.get_event_loop(),
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=app['value_deserializer'],
                retry_backoff_ms=5000
            )
            consumer.subscribe([app['consumer_topic']], listener=NewPartitionsListener(app))
            await consumer.start()

            app['consumer'] = consumer

            asyncio.create_task(consume(app))

            app['partitions'] = consumer.assignment()
            logger.info("Listening to partitions: " + str(app['partitions']))

            broker_available = True
            logger.info('Consumer started!')

        except (KafkaConnectionError, NoBrokersAvailable):
            await consumer.stop()
            time.sleep(4)
            continue


async def create_producer(app: web.Application):
    logger = app['logger']
    logger.info('Starting kafka producer...')

    broker_available = False

    while not broker_available:
        try:
            producer = AIOKafkaProducer(
                loop=asyncio.get_event_loop(),
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=app['value_serializer']
            )

            await producer.start()

            app['producer'] = producer

            logger.info('Producer started!')

            broker_available = True

        except (KafkaConnectionError, NoBrokersAvailable):
            await producer.stop()
            time.sleep(4)
            continue


class NewPartitionsListener(ConsumerRebalanceListener):
    def __init__(self, app):
        self.app = app

    async def on_partitions_revoked(self, revoked):
        pass

    async def on_partitions_assigned(self, assigned):
        self.app['partitions'] = assigned
        self.app['logger'].info("Listening to partitions: " + str(self.app['partitions']))


async def consume(app: web.Application):
    logger = app['logger']
    consumer = app['consumer']
    logger.info('Starting to consume messages...')
    async for msg in consumer:
        key = msg.key.decode('utf-8')
        if msg.key.decode('utf-8') in app['messages']:
            value = msg.value
            # logger.info(f'Consumer received message - {key}, {value}')

            if app['messages'][key].done():
                logger.warning(f'Future was already done for request id {key}')
            else:
                app['messages'][key].set_result(value)
        else:
            logger.warning(f'Received message for an unknown request id: {key}')


async def shutdown_kafka(app: web.Application):
    logger = app['logger']
    logger.info('Stopping consumer and producer...')
    await app['consumer'].stop()
    await app['producer'].stop()
    logger.info('Stopped consumer and producer!')


async def produce_and_wait_for_response(app, key, value):
    logger = app['logger']
    request_id = value.request_id
    k = str(key).encode('utf-8')
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    app['messages'][request_id] = future

    await app['producer'].send_and_wait(app['producer_topic'], key=k, value=value)

    try:
        result = await asyncio.wait_for(future, timeout=app['timeout'])
        del app['messages'][request_id]
        return result

    except asyncio.TimeoutError:
        logger.error(f'Timeout while waiting for message with request id: {request_id}')
        del app['messages'][request_id]
        raise


def add_request_id_factory(app):
    @web.middleware
    async def add_request_id(request, handler):
        partitions = [tp.partition for tp in app['partitions'] if tp.topic == app['consumer_topic']]
        request['request_id'] = str(random.choice(partitions)) + "-" + str(uuid.uuid4()).replace('-', '')
        resp = await handler(request)
        return resp
    return add_request_id
