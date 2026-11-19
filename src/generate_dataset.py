import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

OUTPUT = "data/generated"
os.makedirs(OUTPUT, exist_ok=True)

cities = [
    "Hyderabad", "Visakhapatnam", "Vijayawada",
    "Chennai", "Bengaluru", "Mumbai",
    "Delhi", "Pune", "Kolkata", "Kochi"
]

categories = {
    "Electronics": ["Laptop", "Mobile", "Monitor", "Tablet"],
    "Accessories": ["Mouse", "Keyboard", "Headphones", "Webcam"],
    "Home": ["Mixer", "Fan", "Lamp", "Speaker"],
    "Fashion": ["T-Shirt", "Shoes", "Watch", "Bag"]
}

def write_csv(filename, columns, rows):
    with open(os.path.join(OUTPUT, filename), "w") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

customers = []
for i in range(1, 1001):
    customers.append({
        "customer_id": "C{:04d}".format(i),
        "customer_name": "Customer{}".format(i),
        "city": random.choice(cities)
    })

products = []
product_prices = {}

product_id = 1
for category, names in categories.items():
    for name in names:
        for number in range(1, 26):
            pid = "P{:04d}".format(product_id)
            price = round(random.uniform(100, 50000), 2)

            products.append({
                "product_id": pid,
                "product_name": "{} {}".format(name, number),
                "category": category,
                "unit_price": price
            })

            product_prices[pid] = price
            product_id += 1

orders = []
order_items = []

start_date = datetime(2026, 1, 1)

for i in range(1, 5001):
    oid = "O{:06d}".format(i)

    order_date = start_date + timedelta(
        days=random.randint(0, 315)
    )

    orders.append({
        "order_id": oid,
        "customer_id": random.choice(customers)["customer_id"],
        "order_date": order_date.strftime("%Y-%m-%d"),
        "status": random.choices(
            ["COMPLETED", "CANCELLED", "PENDING"],
            weights=[75, 15, 10]
        )[0]
    })

    selected_products = random.sample(
        products,
        random.randint(1, 5)
    )

    for product in selected_products:
        order_items.append({
            "order_id": oid,
            "product_id": product["product_id"],
            "quantity": random.randint(1, 5),
            "unit_price": product_prices[product["product_id"]]
        })

write_csv(
    "customers.csv",
    ["customer_id", "customer_name", "city"],
    customers
)

write_csv(
    "products.csv",
    ["product_id", "product_name", "category", "unit_price"],
    products
)

write_csv(
    "orders.csv",
    ["order_id", "customer_id", "order_date", "status"],
    orders
)

write_csv(
    "order_items.csv",
    ["order_id", "product_id", "quantity", "unit_price"],
    order_items
)

print("===== SYNTHETIC DATASET GENERATED =====")
print("Customers:", len(customers))
print("Products:", len(products))
print("Orders:", len(orders))
print("Order Items:", len(order_items))
print("Output directory:", OUTPUT)
