
import sys
import json
import base64
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda"))

import lambda_function


@pytest.fixture
def aws_resources():
    with mock_aws():
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="ap-south-1"
        )

        table = dynamodb.create_table(
            TableName="ecommerce-order-ledger",
            KeySchema=[
                {"AttributeName": "order_id", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "order_id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        s3 = boto3.client("s3", region_name="ap-south-1")
        bucket = "test-processed-orders"

        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                "LocationConstraint": "ap-south-1"
            }
        )

        lambda_function.ledger = table
        lambda_function.s3 = s3
        lambda_function.BUCKET = bucket

        yield table, s3, bucket


def make_event(order):
    encoded_order = base64.b64encode(
        json.dumps(order).encode("utf-8")
    ).decode("utf-8")

    return {
        "Records": [
            {
                "kinesis": {
                    "data": encoded_order,
                    "sequenceNumber": "123456789"
                }
            }
        ]
    }


def make_valid_order(order_id="ORD_TEST_001"):
    return {
        "order_id": order_id,
        "customer_id": "CUST101",
        "product_id": "PROD101",
        "quantity": 2,
        "price": 1500.0,
        "total_amount": 3000.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }


def test_lambda_processes_valid_order(aws_resources):
    table, s3, bucket = aws_resources

    order = make_valid_order()
    event = make_event(order)

    result = lambda_function.lambda_handler(event, None)

    assert result["batchItemFailures"] == []

    response = s3.get_object(
        Bucket=bucket,
        Key="processed/athena-ready/ORD_TEST_001.json"
    )

    saved_order = json.loads(response["Body"].read())

    assert saved_order["order_id"] == "ORD_TEST_001"

    ledger_item = table.get_item(
        Key={"order_id": "ORD_TEST_001"}
    )["Item"]

    assert ledger_item["status"] == "COMPLETE"
    assert "lease_token" not in ledger_item
    assert "lease_until" not in ledger_item


def test_lambda_skips_completed_duplicate(aws_resources):
    order = make_valid_order("ORD_TEST_002")
    event = make_event(order)

    first_result = lambda_function.lambda_handler(event, None)
    second_result = lambda_function.lambda_handler(event, None)

    assert first_result["batchItemFailures"] == []
    assert second_result["batchItemFailures"] == []


def test_lambda_quarantines_invalid_order(aws_resources):
    table, s3, bucket = aws_resources

    order = make_valid_order("ORD_INVALID_001")

    # Make the order invalid
    order["quantity"] = -5

    event = make_event(order)

    result = lambda_function.lambda_handler(event, None)

    # Invalid order should not cause a Kinesis retry
    assert result["batchItemFailures"] == []

    # Verify the order was written to quarantine
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="processed/quarantine/"
    )

    assert response["KeyCount"] == 1

    # Verify no ledger entry was created
    ledger_item = table.get_item(
        Key={"order_id": "ORD_INVALID_001"}
    )

    assert "Item" not in ledger_item



def test_lambda_reports_failure_when_s3_fails(aws_resources, monkeypatch):
    table, s3, bucket = aws_resources

    order = make_valid_order("ORD_S3_FAIL_001")
    event = make_event(order)

    def failing_put_object(**kwargs):
        raise ClientError(
            {
                "Error": {
                    "Code": "ServiceUnavailable",
                    "Message": "Simulated S3 failure"
                }
            },
            "PutObject"
        )

    monkeypatch.setattr(
        lambda_function.s3,
        "put_object",
        failing_put_object
    )

    result = lambda_function.lambda_handler(event, None)

    # Lambda must report this record for retry
    assert result["batchItemFailures"] == [
        {"itemIdentifier": "123456789"}
    ]

    # The order should remain PROCESSING until its lease expires
    ledger_item = table.get_item(
        Key={"order_id": "ORD_S3_FAIL_001"}
    )["Item"]

    assert ledger_item["status"] == "PROCESSING"
    assert "lease_token" in ledger_item
    assert "lease_until" in ledger_item


def test_lambda_recovers_expired_lease(aws_resources):
    table, s3, bucket = aws_resources

    order = make_valid_order("ORD_RECOVER_001")

    # Simulate an abandoned claim with an expired lease
    table.put_item(
        Item={
            "order_id": "ORD_RECOVER_001",
            "status": "PROCESSING",
            "lease_until": 1,
            "lease_token": "old-invocation-token"
        }
    )

    event = make_event(order)

    result = lambda_function.lambda_handler(event, None)

    assert result["batchItemFailures"] == []

    # Verify the order was successfully written to S3
    response = s3.get_object(
        Bucket=bucket,
        Key="processed/athena-ready/ORD_RECOVER_001.json"
    )

    saved_order = json.loads(response["Body"].read())

    assert saved_order["order_id"] == "ORD_RECOVER_001"

    # Verify the new invocation completed the order
    ledger_item = table.get_item(
        Key={"order_id": "ORD_RECOVER_001"}
    )["Item"]

    assert ledger_item["status"] == "COMPLETE"
    assert "lease_token" not in ledger_item
    assert "lease_until" not in ledger_item
