
import boto3
import json
from datetime import datetime, timezone

kinesis = boto3.client(
    "kinesis",
    region_name="ap-south-1"
)

invalid_order = {
    "order_id": "ORD9002",
    "customer_id": "CUST101",
    "product_id": "PROD501",
    "quantity": 2,
    "price": 1500,
    "total_amount": 2000,
    "order_status": "PLACED",
    "event_type": "ORDER_CREATED",
    "event_timestamp": datetime.now(timezone.utc).isoformat()
}

response = kinesis.put_record(
    StreamName="ecommerce-orders",
    Data=json.dumps(invalid_order).encode("utf-8"),
    PartitionKey="CUST101"
)

print("Invalid test order sent to Kinesis!")
print("Sequence number:", response["SequenceNumber"])
