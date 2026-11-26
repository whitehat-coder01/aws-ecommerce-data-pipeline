
import json
import boto3
import base64
import uuid
import math
from decimal import Decimal, InvalidOperation

from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
ledger = dynamodb.Table("ecommerce-order-ledger")
s3 = boto3.client("s3")

BUCKET = "ecommerce-processed-orders-harsh"


def validate_order(order):
    errors = []

    if not isinstance(order, dict):
        return ["Order must be a JSON object"]

    # Required string fields
    for field in ["order_id", "customer_id", "product_id"]:
        if not isinstance(order.get(field), str) or not order[field].strip():
            errors.append(f"Missing or invalid {field}")

    # Quantity validation
    quantity = order.get("quantity")

    if isinstance(quantity, bool) or not isinstance(quantity, int):
        errors.append("Quantity must be an integer")
    elif quantity <= 0:
        errors.append("Quantity must be greater than zero")

    # Price validation
    price = order.get("price")

    try:
        price_value = Decimal(str(price))
        if isinstance(price, bool) or not price_value.is_finite() or price_value <= 0:
            errors.append("Price must be a positive number")
    except (InvalidOperation, ValueError, TypeError):
        errors.append("Price must be a valid number")
        price_value = None

    # Total amount validation
    total = order.get("total_amount")

    try:
        total_value = Decimal(str(total))
        if isinstance(total, bool) or not total_value.is_finite() or total_value < 0:
            errors.append("Total amount must be a valid non-negative number")
    except (InvalidOperation, ValueError, TypeError):
        errors.append("Total amount must be a valid number")
        total_value = None

    if (
        isinstance(quantity, int)
        and not isinstance(quantity, bool)
        and quantity > 0
        and price_value is not None
        and total_value is not None
        and price_value.is_finite()
        and price_value > 0
        and total_value.is_finite()
        and total_value >= 0
    ):
        if total_value != quantity * price_value:
            errors.append("Total amount does not equal quantity multiplied by price")

    # Timestamp validation
    timestamp = order.get("event_timestamp")

    if not isinstance(timestamp, str) or not timestamp.strip():
        errors.append("Missing event_timestamp")
    else:
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            errors.append("Invalid event_timestamp format")

    return errors




def lambda_handler(event, context):

    batch_item_failures = []

    for record in event["Records"]:

        sequence_number = record["kinesis"]["sequenceNumber"]

        try:
            raw_data = record["kinesis"]["data"]

            decoded_data = base64.b64decode(
                raw_data,
                validate=True
            ).decode("utf-8")

            order = json.loads(decoded_data)
            errors = validate_order(order)

            # Step 1: Quarantine invalid orders
            if errors:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f"processed/quarantine/{uuid.uuid4()}.json",
                    Body=json.dumps({
                        "raw_data": raw_data,
                        "error": "; ".join(errors)
                    }),
                    ContentType="application/json"
                )

                print(f"Rejected invalid order: {errors}")
                continue

            order_id = order["order_id"]

            now = int(datetime.now(timezone.utc).timestamp())
            lease_until = now + 60
            lease_token = str(uuid.uuid4())

            # Step 2: Create a new processing claim
            try:
                ledger.put_item(
                    Item={
                        "order_id": order_id,
                        "status": "PROCESSING",
                        "created_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "lease_until": lease_until,
                        "lease_token": lease_token
                    },
                    ConditionExpression="attribute_not_exists(order_id)"
                )

            except ClientError as error:

                if error.response["Error"]["Code"] != \
                        "ConditionalCheckFailedException":
                    raise

                # Step 3: Attempt to recover an expired claim
                try:
                    ledger.update_item(
                        Key={"order_id": order_id},
                        UpdateExpression=(
                            "SET lease_until = :lease, "
                            "lease_token = :token"
                        ),
                        ConditionExpression=(
                            "#status = :processing AND "
                            "(attribute_not_exists(lease_until) "
                            "OR lease_until < :now)"
                        ),
                        ExpressionAttributeNames={
                            "#status": "status"
                        },
                        ExpressionAttributeValues={
                            ":processing": "PROCESSING",
                            ":lease": lease_until,
                            ":token": lease_token,
                            ":now": now
                        }
                    )

                except ClientError as recovery_error:

                    if recovery_error.response["Error"]["Code"] != \
                            "ConditionalCheckFailedException":
                        raise

                    existing = ledger.get_item(
                        Key={"order_id": order_id},
                        ConsistentRead=True
                    ).get("Item")

                    if existing and existing.get("status") == "COMPLETE":
                        print(f"Duplicate order skipped: {order_id}")
                        continue

                    print(
                        f"Order currently processing; retry later: "
                        f"{order_id}"
                    )

                    batch_item_failures.append({
                        "itemIdentifier": sequence_number
                    })
                    continue

            # Step 4: Verify ownership immediately before S3 write
            ledger.update_item(
                Key={"order_id": order_id},
                UpdateExpression="SET last_verified_at = :now",
                ConditionExpression=(
                    "#status = :processing AND "
                    "lease_token = :token AND "
                    "lease_until >= :now"
                ),
                ExpressionAttributeNames={
                    "#status": "status"
                },
                ExpressionAttributeValues={
                    ":processing": "PROCESSING",
                    ":token": lease_token,
                    ":now": int(
                        datetime.now(timezone.utc).timestamp()
                    )
                }
            )

            # Step 5: Write valid order to S3
            output_key = (
                f"processed/athena-ready/{order_id}.json"
            )

            s3.put_object(
                Bucket=BUCKET,
                Key=output_key,
                Body=json.dumps(order),
                ContentType="application/json"
            )

            # Step 6: Mark complete only if this invocation owns the claim
            ledger.update_item(
                Key={"order_id": order_id},
                UpdateExpression=(
                    "SET #status = :status "
                    "REMOVE lease_until, lease_token"
                ),
                ConditionExpression=(
                    "#status = :processing AND "
                    "lease_token = :token AND "
                    "lease_until >= :now"
                ),
                ExpressionAttributeNames={
                    "#status": "status"
                },
                ExpressionAttributeValues={
                    ":status": "COMPLETE",
                    ":processing": "PROCESSING",
                    ":token": lease_token,
                    ":now": int(
                        datetime.now(timezone.utc).timestamp()
                    )
                }
            )

            print(f"Successfully processed order: {order_id}")

        except Exception as error:

            print(
                f"Processing failed for sequence "
                f"{sequence_number}: {error}"
            )

            batch_item_failures.append({
                "itemIdentifier": sequence_number
            })

    return {
        "batchItemFailures": batch_item_failures
    }

