
import json
import random
import boto3
from datetime import datetime, timezone

# Configuration
BUCKET_NAME = "ecommerce-data-lake-659817810045"
REGION = "ap-south-1"
TOTAL_ORDERS = 1000

# Sample products
products = [
    {"product_id": "PROD501", "price": 1500},
    {"product_id": "PROD502", "price": 2500},
    {"product_id": "PROD503", "price": 3500},
    {"product_id": "PROD504", "price": 500},
    {"product_id": "PROD505", "price": 1000}
]

# Connect to S3
s3 = boto3.client("s3", region_name=REGION)

# Generate orders
orders = []

for i in range(1, TOTAL_ORDERS + 1):

    product = random.choice(products)
    quantity = random.randint(1, 5)

    order = {
        "order_id": f"ORD{i:06d}",
        "customer_id": f"CUST{random.randint(101, 500)}",
        "product_id": product["product_id"],
        "quantity": quantity,
        "price": product["price"],
        "total_amount": quantity * product["price"],
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }

    orders.append(order)

# Save locally in JSON Lines format
file_name = "orders_1000.jsonl"

with open(file_name, "w", encoding="utf-8") as file:
    for order in orders:
        file.write(json.dumps(order) + "\n")

print(f"Generated {len(orders)} orders.")

# Upload generated data to S3
s3_key = "raw/orders/batch/orders_1000.jsonl"

s3.upload_file(
    file_name,
    BUCKET_NAME,
    s3_key
)

print("Successfully uploaded orders to S3!")
print("S3 location:", s3_key)
