import requests

class UserClient:
    def __init__(self, url='http://127.0.0.1:81/user'):
        self.url = url

    def get(self, id):
        return requests.get(self.url + '/get/' + str(id))

    def create(self):
        return requests.get(self.url + '/create')

    def delete(self, id):
        return requests.get(self.url + '/delete/' + str(id))

    def add_credit(self, id, amount):
        return requests.get(self.url + '/add_credit/' + str(id) 
            + '/' + str(amount))
    
    def subtract_credit(self, id, amount):
        return requests.get(self.url + '/subtract_credit/' + str(id) 
            + '/' + str(amount))


class StockClient:
    def __init__(self, url='http://127.0.0.1:82/stock'):
        self.url = url
    
    def get(self, id):
        return requests.get(self.url + '/get/' + str(id))

    def create(self, price):
        return requests.get(self.url + '/create/' + str(price))

    def delete(self, id):
        return requests.get(self.url + '/delete/' + str(id))

    def add_stock(self, id, amount):
        return requests.get(self.url + '/add_stock/' + str(id) 
            + '/' + str(amount))
    
    def subtract_stock(self, id, amount):
        return requests.get(self.url + '/subtract_stock/' + str(id) 
            + '/' + str(amount))


class OrderClient:
    def __init__(self, url='http://127.0.0.1:83/order'):
        self.url = url
    
    def get(self, id):
        return requests.get(self.url + '/get/' + str(id))

    def create(self, user_id):
        return requests.get(self.url + '/create/' + str(user_id))

    def delete(self, id):
        return requests.get(self.url + '/delete/' + str(id))

    def add_product(self, id, product_id, amount):
        return requests.get(self.url + '/add/' + str(id) 
            + '/' + str(product_id) + '/' + str(amount))
    
    def subtract_product(self, id, product_id, amount):
        return requests.get(self.url + '/subtract/' + str(id) 
            + '/' + str(product_id) + '/' + str(amount))

    def checkout(self, id):
        return requests.get(self.url + '/checkout/' + str(id))

    def cancel_checkout(self, id):
        return requests.get(self.url + '/cancel_checkout/' + str(id))