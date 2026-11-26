
import boto3
import json
from datetime import datetime, timezone

kinesis = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

timestamp = datetime.now(timezone.utc).isoformat()

test_orders = [
    {
        "order_id": "ORD9101",
        "customer_id": "",
        "product_id": "PROD501",
        "quantity": 2,
        "price": 1500,
        "total_amount": 3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp
    },
    {
        "order_id": "ORD9102",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": -2,
        "price": 1500,
        "total_amount": 3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp
    },
    {
        "order_id": "ORD9103",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": 2,
        "price": -1500,
        "total_amount": -3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp
    },
    {
        "order_id": "ORD9104",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": 2,
        "price": 1500,
        "total_amount": 2000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp
    },
    {
        "order_id": "ORD9105",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": 2,
        "price": 1500,
        "total_amount": 3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": "invalid-date"
    },
    {
        "order_id": "ORD9106",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": 2,
        "price": 1500,
        "total_amount": 3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": timestamp
    }
]

for order in test_orders:
    response = kinesis.put_record(
        StreamName="ecommerce-orders",
        Data=json.dumps(order).encode("utf-8"),
        PartitionKey=order["customer_id"] or "MISSING"
    )

    print(
        f"Sent {order['order_id']} | "
        f"Sequence: {response['SequenceNumber']}"
    )

print("All 6 data quality test orders submitted!")
