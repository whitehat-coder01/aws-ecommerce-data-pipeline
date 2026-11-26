
import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def aws_environment():
    with mock_aws():
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="ap-south-1"
        )

        table = dynamodb.create_table(
            TableName="ecommerce-order-ledger",
            KeySchema=[
                {
                    "AttributeName": "order_id",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "order_id",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        yield table


def test_first_order_claim(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9501",
            "status": "PROCESSING",
            "lease_until": 9999999999
        },
        ConditionExpression="attribute_not_exists(order_id)"
    )

    result = table.get_item(
        Key={"order_id": "ORD9501"}
    )

    assert result["Item"]["status"] == "PROCESSING"


def test_duplicate_order_claim_rejected(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9502",
            "status": "COMPLETE"
        }
    )

    with pytest.raises(Exception):
        table.put_item(
            Item={
                "order_id": "ORD9502",
                "status": "PROCESSING"
            },
            ConditionExpression="attribute_not_exists(order_id)"
        )


def test_expired_lease_recovery(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9503",
            "status": "PROCESSING",
            "lease_until": 100
        }
    )

    now = 200
    new_lease = 260

    table.update_item(
        Key={"order_id": "ORD9503"},
        UpdateExpression="SET lease_until = :lease",
        ConditionExpression=(
            "#status = :processing AND lease_until < :now"
        ),
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues={
            ":processing": "PROCESSING",
            ":lease": new_lease,
            ":now": now
        }
    )

    result = table.get_item(
        Key={"order_id": "ORD9503"}
    )

    assert result["Item"]["lease_until"] == new_lease


def test_active_lease_cannot_be_recovered(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9504",
            "status": "PROCESSING",
            "lease_until": 300
        }
    )

    with pytest.raises(Exception):
        table.update_item(
            Key={"order_id": "ORD9504"},
            UpdateExpression="SET lease_until = :lease",
            ConditionExpression=(
                "#status = :processing AND lease_until < :now"
            ),
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":processing": "PROCESSING",
                ":lease": 400,
                ":now": 200
            }
        )

    result = table.get_item(
        Key={"order_id": "ORD9504"}
    )

    assert result["Item"]["lease_until"] == 300



def test_old_owner_cannot_complete_order(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9505",
            "status": "PROCESSING",
            "lease_until": 300,
            "lease_token": "new-owner"
        }
    )

    with pytest.raises(Exception):
        table.update_item(
            Key={"order_id": "ORD9505"},
            UpdateExpression="SET #status = :status",
            ConditionExpression=(
                "#status = :processing AND lease_token = :token"
            ),
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":status": "COMPLETE",
                ":processing": "PROCESSING",
                ":token": "old-owner"
            }
        )

    result = table.get_item(
        Key={"order_id": "ORD9505"}
    )

    assert result["Item"]["status"] == "PROCESSING"
    assert result["Item"]["lease_token"] == "new-owner"


def test_old_owner_cannot_delete_new_claim(aws_environment):
    table = aws_environment

    table.put_item(
        Item={
            "order_id": "ORD9506",
            "status": "PROCESSING",
            "lease_until": 300,
            "lease_token": "new-owner"
        }
    )

    with pytest.raises(Exception):
        table.delete_item(
            Key={"order_id": "ORD9506"},
            ConditionExpression=(
                "#status = :processing AND lease_token = :token"
            ),
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":processing": "PROCESSING",
                ":token": "old-owner"
            }
        )

    result = table.get_item(
        Key={"order_id": "ORD9506"}
    )

    assert result["Item"]["lease_token"] == "new-owner"


def test_only_one_owner_can_claim_order(aws_environment):
    table = aws_environment

    # First invocation claims the order
    table.put_item(
        Item={
            "order_id": "ORD9507",
            "status": "PROCESSING",
            "lease_until": 9999999999,
            "lease_token": "invocation-A"
        },
        ConditionExpression="attribute_not_exists(order_id)"
    )

    # Second invocation attempts to claim the same order
    with pytest.raises(Exception):
        table.put_item(
            Item={
                "order_id": "ORD9507",
                "status": "PROCESSING",
                "lease_until": 9999999999,
                "lease_token": "invocation-B"
            },
            ConditionExpression="attribute_not_exists(order_id)"
        )

    # Verify that the first invocation still owns the order
    result = table.get_item(
        Key={"order_id": "ORD9507"}
    )

    assert result["Item"]["lease_token"] == "invocation-A"
    assert result["Item"]["status"] == "PROCESSING"
