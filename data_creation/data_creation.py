import os
import time
import random
from datetime import datetime
import psycopg2
from faker import Faker
import uuid

faker = Faker()


class Customer:
    def __init__(self):
        self.id = uuid.uuid4()
        self.created_time = datetime.now()
        self.full_name = faker.name()
        self.salulation = None
        self.first_name = None
        self.last_name = None
        self.full_address = faker.address()
        self.street_number = None
        self.street_name = None
        self.postcode = None
        self.state = None
        self.country = None


class Orders:
    def __init__(self, new_customer: bool, transaction: bool):
        self.new_customer = new_customer
        self.new_transaction = new_transaction
        self


def create_transactions():
    return


def create_user():
    return


def create_product():
    return
