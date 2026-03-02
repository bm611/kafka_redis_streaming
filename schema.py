BOOKING_EVENT_SCHEMA = {
    "required_fields": {
        "booking_id": str,
        "timestamp": str,
        "user_id": str,
        "hotel_name": str,
        "room_type": str,
        "check_in_date": str,
        "check_out_date": str,
        "total_price_usd": (int, float),
        "payment_method": str,
        "country_of_user": str,
    },
    "allowed_room_types": {"Standard", "Deluxe", "Suite"},
    "allowed_payment_methods": {"credit_card", "paypal", "debit_card"},
}


def validate_booking_event(event: dict) -> list[str]:
    """Validate a booking event against the schema. Returns a list of errors (empty = valid)."""
    errors = []
    schema = BOOKING_EVENT_SCHEMA

    for field, expected_type in schema["required_fields"].items():
        if field not in event:
            errors.append(f"missing required field: {field}")
        elif not isinstance(event[field], expected_type):
            errors.append(f"{field}: expected {expected_type}, got {type(event[field])}")

    if not errors:
        if event["room_type"] not in schema["allowed_room_types"]:
            errors.append(f"invalid room_type: {event['room_type']}")
        if event["payment_method"] not in schema["allowed_payment_methods"]:
            errors.append(f"invalid payment_method: {event['payment_method']}")
        if event["total_price_usd"] < 0:
            errors.append("total_price_usd cannot be negative")

    return errors
