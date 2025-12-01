import random
from datetime import datetime
from faker import Faker
import uuid
import json


faker = Faker()


class Customer:
    def __init__(self, name, address):
        self.id = str(uuid.uuid4())
        self.created_time = str(datetime.now())
        self.full_name = str(name)
        self.salulation = self.get_salutation()
        self.first_name = self.get_first_name()
        self.last_name = self.get_last_name()
        self.full_address = str(address)
        self.street_number = self.get_address_number()
        self.street_name = self.get_street_name()
        self.city = self.get_city()
        self.postcode = self.get_postcode()
        self.state = self.get_state()

    def get_salutation(self) -> str:
        if self.full_name is None:
            return ""
        name_parts = self.full_name.split(" ")
        if name_parts[0].__contains__("."):
            return name_parts[0]
        return ""

    def get_first_name(self) -> str:
        if self.full_name is None:
            return ""
        name_parts = self.full_name.split(" ")
        if name_parts[0].__contains__("."):
            return name_parts[1]
        return name_parts[0]

    def get_last_name(self) -> str:
        if self.full_name is None:
            return ""
        name_parts = self.full_name.split(" ")
        if name_parts[0].__contains__("."):
            return name_parts[2]
        return name_parts[1]

    def get_address_number(self) -> int:
        try:
            num = int(self.full_address.split(" ")[0])
            return num
        except Exception:
            return 9999999999

    def get_street_name(self) -> str:
        addy = self.full_address.split(",")
        return " ".join(addy[0].split(" ")[1:-1])

    def get_city(self) -> str:
        addy = self.full_address.split(",")
        return addy[0].split(" ")[-1]

    def get_postcode(self) -> str:
        try:
            addy = self.full_address.split(",")
            return addy[1].split()[-1]
        except Exception as e:
            return ""

    def get_state(self) -> str:
        try:
            addy = self.full_address.split(",")
            return addy[1].split()[0]
        except Exception as e:
            return ""


class Orders:
    def __init__(self, customer: Customer):
        self.created_at = str(datetime.now())
        self.id = str(uuid.uuid4())
        self.customer_id = customer.id
        self.order_products = self.get_product_orders()
        self.order_quantity = self.get_order_quantity()

    def get_product_orders(self) -> dict:
        prod_dict = {}
        prods = random.randint(1, 5)
        while prods != 0:
            num = random.randint(1, 10)
            if num in (1, 2):
                prod_dict["Mason Margiela Tabi Boots"] = 1500.00
            if num in (3, 4):
                prod_dict["Our Legacy Brick Grande"] = 1300.00
            if num in (5, 6):
                prod_dict["Margerat Howell Check Shirt"] = 430.00
            if num in (7, 8):
                prod_dict["mfpen Service Trouser"] = 549.00
            if num in (9, 10):
                prod_dict["Studio Nicholson Lay Boxy Fit T-Shirt"] = 190.00
            prods -= 1
        return prod_dict

    def get_order_quantity(self) -> dict:
        order_pty_dict = {}
        for key, _ in self.order_products.items():
            order_pty_dict[key] = random.randint(1, 6)
        return order_pty_dict


def dict_to_json_file(dt: dict, name: str):
    id = uuid.uuid4()
    with open(f"data/{name}_{id}.json", "w") as f:
        json.dump(dt, f)


if __name__ == "__main__":
    customers = [
        Customer(faker.name(), faker.address().replace("\n", " ")) for c in range(50)
    ]
    for _ in range(10):
        rn = random.randint(1, 10)
        if rn <= 5:
            customer = Customer(faker.name(), faker.address().replace("\n", " "))
            customers.append(customer)
            if rn < 3:
                order = Orders(customer)
                dict_to_json_file(order.__dict__, "order")
            dict_to_json_file(customer.__dict__, "customer")
        else:
            customer = random.choice(customers)
            order = Orders(customer)
            dict_to_json_file(order.__dict__, "order")
