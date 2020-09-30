from clients import StockClient, UserClient, OrderClient

import pytest


# User fixtures
@pytest.fixture
def user_client():
    return UserClient()

@pytest.fixture
def user_id(user_client):
    response = user_client.create()
    user_id = response.json()['userId']
    return user_id

@pytest.fixture
def deleted_user_id(user_client, user_id):
    user_client.delete(user_id)
    return user_id


# Stock fixtures
@pytest.fixture
def stock_client():
    return StockClient()

@pytest.fixture
def product_id(stock_client):
    response = stock_client.create(1)
    product_id = response.json()['productId']
    return product_id

@pytest.fixture
def deleted_product_id(stock_client, product_id):
    stock_client.delete(product_id)
    return product_id


# Order fixtures
@pytest.fixture
def order_client():
    return OrderClient()

@pytest.fixture
def order_id(order_client, user_id):
    response = order_client.create(user_id)
    order_id = response.json()['orderId']
    return order_id

@pytest.fixture
def deleted_order_id(order_client, order_id):
    order_client.delete(order_id)
    return order_id