def test_get_non_existing(stock_client):
    response = stock_client.get(1)
    assert response.status_code == 500

def test_delete_non_existing(stock_client):
    response = stock_client.delete(2)
    assert response.status_code == 500

def test_add_stock_non_existing(stock_client):
    response = stock_client.add_stock(3, 10)
    assert response.status_code == 500

def test_subtract_stock_non_existing(stock_client):
    response = stock_client.subtract_stock(4, 10)
    assert response.status_code == 500

def test_create_product(stock_client):
    response = stock_client.create(10)
    assert response.status_code == 200

def test_get_product(stock_client, product_id):
    response = stock_client.get(product_id)
    assert response.status_code == 200
    assert response.json()['productId'] == product_id

def test_get_price(stock_client, product_id):
    price = 10
    response = stock_client.create(price)
    product_id = response.json()['productId']
    response = stock_client.get(product_id)
    assert response.status_code == 200
    assert response.json()['productId'] == product_id
    assert response.json()['state']['price'] == price

def test_delete_product(stock_client, product_id):
    response = stock_client.delete(product_id)
    assert response.status_code == 200
    response = stock_client.get(product_id)
    assert response.status_code == 500

def test_add_stock_deleted(stock_client, deleted_product_id):
    response = stock_client.add_stock(deleted_product_id, 10)
    assert response.status_code == 500

def test_subtract_stock_deleted(stock_client, deleted_product_id):
    response = stock_client.subtract_stock(deleted_product_id, 10)
    assert response.status_code == 500

def test_add_stock(stock_client, product_id):
    amount = 10
    response = stock_client.add_stock(product_id, amount)
    assert response.status_code == 200
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == amount

def test_subtract_stock(stock_client, product_id):
    amount_added = 10
    amount_subtracted = 8
    stock_client.add_stock(product_id, amount_added)
    response = stock_client.subtract_stock(product_id, amount_subtracted)
    assert response.status_code == 200
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == amount_added - amount_subtracted

def test_subtract_stock_multiple_times(stock_client, product_id):
    amount_added = 10
    amount_subtracted = 3
    stock_client.add_stock(product_id, amount_added)
    stock_client.subtract_stock(product_id, amount_subtracted)
    response = stock_client.subtract_stock(product_id, amount_subtracted)
    assert response.status_code == 200
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == amount_added - 2 * amount_subtracted

def test_no_stock(stock_client, product_id):
    amount_subtracted = 3
    response = stock_client.subtract_stock(product_id, amount_subtracted)
    assert response.status_code == 500
    response = stock_client.get(product_id)
    assert 'stock' not in response.json()['state']

def test_not_enough_stock(stock_client, product_id):
    amount_added = 2
    amount_subtracted = 3
    stock_client.add_stock(product_id, amount_added)
    response = stock_client.subtract_stock(product_id, amount_subtracted)
    assert response.status_code == 500
    response = stock_client.get(product_id)
    assert response.json()['state']['stock'] == amount_added