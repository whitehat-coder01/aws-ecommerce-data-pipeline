
import boto3
import json
from datetime import datetime, timezone

# Connect to Amazon S3
s3 = boto3.client(
    "s3",
    region_name="ap-south-1"
)

# Your existing S3 bucket name
BUCKET_NAME = "ecommerce-data-lake-659817810045"

# Sample eCommerce order event
order = {
    "order_id": "ORD1001",
    "customer_id": "CUST101",
    "product_id": "PROD501",
    "quantity": 2,
    "price": 1500,
    "order_status": "PLACED",
    "event_type": "ORDER_CREATED",
    "event_timestamp": datetime.now(
        timezone.utc
    ).isoformat()
}

# Convert Python dictionary into JSON
order_json = json.dumps(order, indent=4)

# Define the S3 object location
file_name = "raw/orders/ORD1001.json"

# Upload JSON data to S3
response = s3.put_object(
    Bucket=BUCKET_NAME,
    Key=file_name,
    Body=order_json,
    ContentType="application/json"
)

print("Order uploaded successfully!")
print("S3 Location:", file_name)
print("HTTP Status:", response["ResponseMetadata"]["HTTPStatusCode"])
