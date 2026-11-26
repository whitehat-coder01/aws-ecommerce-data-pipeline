
import boto3
import json
from datetime import datetime, timezone
import time

kinesis = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

order = {
    "order_id": "ORD9201",
    "customer_id": "CUST101",
    "product_id": "PROD501",
    "quantity": 2,
    "price": 1500,
    "total_amount": 3000,
    "order_status": "PLACED",
    "event_type": "ORDER_CREATED",
    "event_timestamp": datetime.now(timezone.utc).isoformat()
}

for attempt in range(2):
    response = kinesis.put_record(
        StreamName="ecommerce-orders",
        Data=json.dumps(order).encode("utf-8"),
        PartitionKey="CUST101"
    )

    print(f"Attempt {attempt + 1}: Order sent")
    time.sleep(2)

print("Both events submitted!")
