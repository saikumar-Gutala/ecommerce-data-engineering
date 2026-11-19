import csv
import os
import sys
from datetime import datetime

BASE_DIR = os.environ.get("DATA_DIR", "/home/hdpuser/ecommerce-data-engineering/data/raw")
PROJECT_HOME = "/home/hdpuser/ecommerce-data-engineering"
REPORT_FILE = os.environ.get("REPORT_FILE", os.path.join(PROJECT_HOME, "logs/data_quality_report.txt"))

REQUIRED_COLUMNS = {
    "customers.csv": ["customer_id", "customer_name", "city"],
    "products.csv": ["product_id", "product_name", "category", "unit_price"],
    "orders.csv": ["order_id", "customer_id", "order_date", "status"],
    "order_items.csv": ["order_id", "product_id", "quantity", "unit_price"]
}

PRIMARY_KEYS = {
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "orders.csv": "order_id"
}

report = []


def log(message):
    print(message)
    report.append(str(message))


def read_csv(filename):
    path = os.path.join(BASE_DIR, filename)
    with open(path, "r") as file:
        return list(csv.DictReader(file))


def validate_file(filename):
    path = os.path.join(BASE_DIR, filename)

    if not os.path.exists(path):
        log("FAIL: Missing file: " + filename)
        return False

    with open(path, "r") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            log("FAIL: Empty CSV: " + filename)
            return False

        missing = set(REQUIRED_COLUMNS[filename]) - set(reader.fieldnames)

        if missing:
            log("FAIL: Missing columns: " + str(missing))
            return False

        rows = list(reader)

    errors = []

    for index, row in enumerate(rows, start=2):
        for column in REQUIRED_COLUMNS[filename]:
            if not row.get(column) or not row[column].strip():
                errors.append(
                    "Empty value: row {}, column {}".format(index, column)
                )

    key = PRIMARY_KEYS.get(filename)

    if key:
        values = [row[key] for row in rows]
        if len(values) != len(set(values)):
            errors.append("Duplicate primary key: " + key)

    if filename == "orders.csv":
        for index, row in enumerate(rows, start=2):
            try:
                datetime.strptime(row["order_date"], "%Y-%m-%d")
            except ValueError:
                errors.append("Invalid order_date at row {}".format(index))

    if filename == "products.csv":
        for index, row in enumerate(rows, start=2):
            try:
                if float(row["unit_price"]) < 0:
                    errors.append("Negative product price at row {}".format(index))
            except ValueError:
                errors.append("Invalid product price at row {}".format(index))

    if filename == "order_items.csv":
        for index, row in enumerate(rows, start=2):
            try:
                if int(row["quantity"]) <= 0:
                    errors.append("Invalid quantity at row {}".format(index))
                if float(row["unit_price"]) < 0:
                    errors.append("Negative unit price at row {}".format(index))
            except ValueError:
                errors.append("Invalid numeric value at row {}".format(index))

    log("{} | Records: {}".format(filename, len(rows)))

    for error in errors:
        log("FAIL: " + error)

    if errors:
        return False

    log("PASS: " + filename)
    return True


def validate_relationships():
    log("\n===== REFERENTIAL INTEGRITY CHECK =====")

    customers = read_csv("customers.csv")
    products = read_csv("products.csv")
    orders = read_csv("orders.csv")
    items = read_csv("order_items.csv")

    customer_ids = set(row["customer_id"] for row in customers)
    product_ids = set(row["product_id"] for row in products)
    order_ids = set(row["order_id"] for row in orders)

    checks = [
        ("Orders -> Customers",
         [row["customer_id"] for row in orders], customer_ids),
        ("Order Items -> Orders",
         [row["order_id"] for row in items], order_ids),
        ("Order Items -> Products",
         [row["product_id"] for row in items], product_ids)
    ]

    all_passed = True

    for name, foreign_keys, valid_keys in checks:
        invalid = sorted(set(foreign_keys) - valid_keys)

        if invalid:
            log("FAIL: {} | Invalid IDs: {}".format(name, invalid))
            all_passed = False
        else:
            log("PASS: " + name)

    return all_passed


def main():
    log("===== E-COMMERCE DATA QUALITY REPORT =====")
    log("Execution time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    file_results = [
        validate_file(filename)
        for filename in REQUIRED_COLUMNS
    ]

    relationship_result = validate_relationships()

    passed = sum(file_results) + int(relationship_result)
    total = len(file_results) + 1
    failed = total - passed

    log("\n==========================================")
    log("Validation checks passed: {}".format(passed))
    log("Validation checks failed: {}".format(failed))

    if failed:
        log("DATA QUALITY CHECK FAILED")
    else:
        log("DATA QUALITY CHECK PASSED")

    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)

    with open(REPORT_FILE, "w") as file:
        file.write("\n".join(report) + "\n")

    print("\nReport saved to:", REPORT_FILE)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
