from multiprocessing.pool import ThreadPool
import time

def test_get_non_existing(order_client):
    response = order_client.get(1)
    assert response.status_code == 500

def test_delete_non_existing(order_client):
    response = order_client.delete(2)
    assert response.status_code == 500

def test_add_product_non_existing(order_client):
    response = order_client.add_product(3, 10, 10)
    assert response.status_code == 500

def test_subtract_product_non_existing(order_client):
    response = order_client.subtract_product(4, 10, 10)
    assert response.status_code == 500

def test_create_wrong_user_id(order_client):
    response = order_client.create(10)
    assert response.status_code == 500

def test_create_order(order_client, user_id):
    response = order_client.create(user_id)
    assert response.status_code == 200

def test_get_order(order_client, order_id):
    response = order_client.get(order_id)
    assert response.status_code == 200
    assert response.json()['orderId'] == order_id

def test_delete_product(order_client, order_id):
    response = order_client.delete(order_id)
    assert response.status_code == 200
    response = order_client.get(order_id)
    print(str(response))
    assert response.status_code == 500

def test_add_product_deleted(order_client, product_id, deleted_order_id):
    response = order_client.add_product(deleted_order_id, product_id, 10)
    assert response.status_code == 500

def test_subtract_product_deleted(order_client, product_id, deleted_order_id):
    response = order_client.subtract_product(deleted_order_id, product_id, 10)
    assert response.status_code == 500

def test_add_product(order_client, stock_client, product_id, order_id):
    amount = 10
    response = order_client.add_product(order_id, product_id, amount)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['orderItems'][product_id]['amount'] == amount

def test_subtract_product(order_client, order_id, product_id):
    amount_added = 10
    amount_subtracted = 8
    order_client.add_product(order_id, product_id, amount_added)
    response = order_client.subtract_product(order_id, product_id, amount_subtracted)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['orderItems'][product_id]['amount'] == amount_added - amount_subtracted

def test_subtract_order_multiple_times(order_client, order_id, product_id):
    amount_added = 10
    amount_subtracted = 3
    order_client.add_product(order_id, product_id, amount_added)
    order_client.subtract_product(order_id, product_id, amount_subtracted)
    response = order_client.subtract_product(order_id, product_id, amount_subtracted)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['orderItems'][product_id]['amount'] == amount_added - 2 * amount_subtracted

def test_subtract_not_added_product(order_client, order_id, product_id):
    amount_subtracted = 3
    response = order_client.subtract_product(order_id, product_id, amount_subtracted)
    assert response.status_code == 500
    response = order_client.get(order_id)
    assert 'orderItems' not in response.json()['state']

def test_not_enough_added_products(order_client, order_id, product_id):
    amount_added = 2
    amount_subtracted = 3
    order_client.add_product(order_id, product_id, amount_added)
    response = order_client.subtract_product(order_id, product_id, amount_subtracted)
    assert response.status_code == 500
    response = order_client.get(order_id)
    assert response.json()['state']['orderItems'][product_id]['amount'] == amount_added

def test_non_existing_product(order_client, order_id):
    response = order_client.add_product(order_id, 2, 10)
    assert response.status_code == 500

def test_correct_price(order_client, stock_client, order_id, product_id):
    response = stock_client.get(product_id)
    price = response.json()['state']['price']
    order_client.add_product(order_id, product_id, 5)
    response = order_client.get(order_id)
    assert response.status_code == 200
    assert response.json()['state']['orderItems'][product_id]['priceEach'] == price

def test_correct_checkout(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 11)
    user_client.add_credit(user_id, 11)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['paid'] == True
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 1
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 1

def test_correct_checkout_two(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 12)
    user_client.add_credit(user_id, 15)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['paid'] == True
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 5
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 2

def test_correct_and_failed_checkout(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 12)
    user_client.add_credit(user_id, 15)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['paid'] == True
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 5
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 2
    response = order_client.checkout(order_id)
    assert response.status_code == 500
    response = order_client.get(order_id)
    assert response.json()['state']['paid'] == True
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 5
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 2

def test_no_stock_checkout(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 5)
    user_client.add_credit(user_id, 10)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 500
    response = order_client.get(order_id)
    assert 'paid' not in response.json()['state']
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 10
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 5


def test_no_credit_checkout(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 10)
    user_client.add_credit(user_id, 5)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 500
    response = order_client.get(order_id)
    assert 'paid' not in response.json()['state']
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 5
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 10


def test_no_credit_checkout_twice(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 10)
    user_client.add_credit(user_id, 5)
    order_client.add_product(order_id, product_id, 10)
    for i in [0, 1]:
        response = order_client.checkout(order_id)
        assert response.status_code == 500
        response = order_client.get(order_id)
        assert 'paid' not in response.json()['state']
        response = user_client.get(user_id)
        assert response.json()['state']['credit'] == 5
        response = stock_client.get(product_id)
        assert response.json()['state']['stock'] == 10


def test_no_credit_checkout_concurrent(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 10)
    user_client.add_credit(user_id, 5)
    order_client.add_product(order_id, product_id, 10)
    def test(i):
        response = order_client.checkout(order_id)
        assert response.status_code == 500
        return i

    p = ThreadPool(6)
    res = p.map(test, range(200))

    time.sleep(2)
    response = order_client.get(order_id)
    assert 'paid' not in response.json()['state']
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 5
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 10


def test_cancel_checkout(order_client, stock_client, user_client, order_id, product_id, user_id):
    stock_client.add_stock(product_id, 11)
    user_client.add_credit(user_id, 11)
    order_client.add_product(order_id, product_id, 10)
    response = order_client.checkout(order_id)
    assert response.status_code == 200
    response = order_client.get(order_id)
    assert response.json()['state']['paid'] == True
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 1
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 1
    order_client.cancel_checkout(order_id)
    assert response.status_code == 200
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == 11
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == 11
    response = order_client.get(order_id)
    assert 'paid' not in response.json()['state']
