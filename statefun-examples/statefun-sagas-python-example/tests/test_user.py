def test_get_non_existing(user_client):
    response = user_client.get(1)
    assert response.status_code == 500

def test_delete_non_existing(user_client):
    response = user_client.delete(2)
    assert response.status_code == 500

def test_add_credit_non_existing(user_client):
    response = user_client.add_credit(3, 10)
    assert response.status_code == 500

def test_subtract_credit_non_existing(user_client):
    response = user_client.subtract_credit(4, 10)
    assert response.status_code == 500

def test_create_user(user_client):
    response = user_client.create()
    assert response.status_code == 200

def test_get_user(user_client, user_id):
    response = user_client.get(user_id)
    assert response.status_code == 200
    assert response.json()['userId'] == user_id

def test_delete_user(user_client, user_id):
    response = user_client.delete(user_id)
    assert response.status_code == 200
    response = user_client.get(user_id)
    assert response.status_code == 500

def test_add_credit_deleted(user_client, deleted_user_id):
    response = user_client.add_credit(deleted_user_id, 10)
    assert response.status_code == 500

def test_subtract_credit_deleted(user_client, deleted_user_id):
    response = user_client.subtract_credit(deleted_user_id, 10)
    assert response.status_code == 500

def test_add_credit(user_client, user_id):
    amount = 10
    response = user_client.add_credit(user_id, amount)
    assert response.status_code == 200
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == amount

def test_subtract_credit(user_client, user_id):
    amount_added = 10
    amount_subtracted = 8
    user_client.add_credit(user_id, amount_added)
    response = user_client.subtract_credit(user_id, amount_subtracted)
    assert response.status_code == 200
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == amount_added - amount_subtracted

def test_subtract_credit_multiple_times(user_client, user_id):
    amount_added = 10
    amount_subtracted = 3
    user_client.add_credit(user_id, amount_added)
    user_client.subtract_credit(user_id, amount_subtracted)
    response = user_client.subtract_credit(user_id, amount_subtracted)
    assert response.status_code == 200
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == amount_added - 2 * amount_subtracted

def test_no_credit(user_client, user_id):
    amount_subtracted = 3
    response = user_client.subtract_credit(user_id, amount_subtracted)
    assert response.status_code == 500
    response = user_client.get(user_id)
    assert 'credit' not in response.json()['state']

def test_not_enough_credit(user_client, user_id):
    amount_added = 2
    amount_subtracted = 3
    user_client.add_credit(user_id, amount_added)
    response = user_client.subtract_credit(user_id, amount_subtracted)
    assert response.status_code == 500
    response = user_client.get(user_id)
    assert response.json()['state']['credit'] == amount_added