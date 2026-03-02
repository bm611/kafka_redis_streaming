from typing import Literal
from pydantic import BaseModel, Field, ValidationError

class BookingEvent(BaseModel):
    booking_id: str
    timestamp: str
    user_id: str
    hotel_name: str
    room_type: Literal["Standard", "Deluxe", "Suite"]
    check_in_date: str
    check_out_date: str
    total_price_usd: float = Field(ge=0)
    payment_method: Literal["credit_card", "paypal", "debit_card"]
    country_of_user: str

def validate_booking_event(event: dict) -> list[str]:
    """Validate a booking event against the schema. Returns a list of errors (empty = valid)."""
    try:
        BookingEvent.model_validate(event)
        return []
    except ValidationError as e:
        errors = []
        for error in e.errors():
            loc = ".".join(map(str, error["loc"]))
            msg = error["msg"]
            errors.append(f"{loc}: {msg}")
        return errors
