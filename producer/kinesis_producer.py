
import boto3
import json
import random
import time
from datetime import datetime, timezone

STREAM_NAME = "ecommerce-orders"
REGION = "ap-south-1"

kinesis = boto3.client("kinesis", region_name=REGION)

products = ["PROD501", "PROD502", "PROD503", "PROD504", "PROD505"]
customers = ["CUST101", "CUST102", "CUST103", "CUST104", "CUST105"]

def generate_orders(count=1000):
    orders = []

    for i in range(3001, 3001 + count):
        quantity = random.randint(1, 5)
        price = random.choice([500, 1000, 1500, 2000, 2500])

        order = {
            "order_id": f"ORD{i}",
            "customer_id": random.choice(customers),
            "product_id": random.choice(products),
            "quantity": quantity,
            "price": price,
            "total_amount": quantity * price,
            "order_status": random.choice(
                ["PLACED", "SHIPPED", "DELIVERED"]
            ),
            "event_type": "ORDER_CREATED",
            "event_timestamp": datetime.now(timezone.utc).isoformat()
        }

        orders.append(order)

    return orders


def send_batch(records):
    pending = records

    while pending:
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=pending
        )

        failed_count = response["FailedRecordCount"]
        print(f"Sent batch: {len(pending) - failed_count} successful, "
              f"{failed_count} failed")

        pending = [
            pending[index]
            for index, result in enumerate(response["Records"])
            if "ErrorCode" in result
        ]

        if pending:
            time.sleep(2)


def main():
    orders = generate_orders(1000)

    for start in range(0, len(orders), 500):
        batch = orders[start:start + 500]

        records = [
            {
                "Data": json.dumps(order).encode("utf-8"),
                "PartitionKey": order["customer_id"]
            }
            for order in batch
        ]

        send_batch(records)

    print("Successfully submitted all 1000 orders to Kinesis!")


if __name__ == "__main__":
    main()
