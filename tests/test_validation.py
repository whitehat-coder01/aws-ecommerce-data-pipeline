
import sys
from pathlib import Path

# Import the Lambda validation function
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "lambda")
)

from lambda_function import validate_order

from datetime import datetime, timezone


def valid_order():
    return {
        "order_id": "ORD9301",
        "customer_id": "CUST101",
        "product_id": "PROD501",
        "quantity": 2,
        "price": 1500,
        "total_amount": 3000,
        "order_status": "PLACED",
        "event_type": "ORDER_CREATED",
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }


def test_valid_order():
    order = valid_order()
    assert validate_order(order) == []


def test_missing_customer_id():
    order = valid_order()
    order["customer_id"] = ""

    errors = validate_order(order)

    assert len(errors) > 0


def test_negative_quantity():
    order = valid_order()
    order["quantity"] = -2

    errors = validate_order(order)

    assert len(errors) > 0


def test_negative_price():
    order = valid_order()
    order["price"] = -1500

    errors = validate_order(order)

    assert len(errors) > 0


def test_incorrect_total_amount():
    order = valid_order()
    order["total_amount"] = 2000

    errors = validate_order(order)

    assert len(errors) > 0


def test_invalid_timestamp():
    order = valid_order()
    order["event_timestamp"] = "invalid-date"

    errors = validate_order(order)

    assert len(errors) > 0

def test_zero_quantity():
    order = valid_order()
    order["quantity"] = 0
    assert len(validate_order(order)) > 0


def test_boolean_quantity():
    order = valid_order()
    order["quantity"] = True
    assert len(validate_order(order)) > 0


def test_missing_order_id():
    order = valid_order()
    order["order_id"] = ""
    assert len(validate_order(order)) > 0
