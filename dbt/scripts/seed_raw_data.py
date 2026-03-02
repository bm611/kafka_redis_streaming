"""
Seed raw tables in Postgres with fake booking and ad data.

Usage:
    python scripts/seed_raw_data.py [--bookings 10000] [--impressions 50000]
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
from faker import Faker

fake = Faker()

# --- Constants ---

HOTELS = [
    ("Grand Hyatt Amsterdam", "Netherlands"),
    ("Marriott Barcelona", "Spain"),
    ("Hilton London", "United Kingdom"),
    ("Ibis Paris", "France"),
    ("NH Collection Berlin", "Germany"),
    ("Radisson Blu Rome", "Italy"),
    ("Novotel Lisbon", "Portugal"),
    ("InterContinental Vienna", "Austria"),
]

ROOM_TYPES = ["Standard", "Deluxe", "Suite"]
PAYMENT_METHODS = ["credit_card", "paypal", "debit_card"]

PARTNERS = [
    ("P001", "Google Ads"),
    ("P002", "Meta Ads"),
    ("P003", "TripAdvisor"),
    ("P004", "Kayak"),
    ("P005", "Trivago"),
]

PLACEMENTS = ["search_results", "hotel_page", "checkout", "homepage_banner", "map_view"]

# --- Generators ---


def generate_bookings(n: int) -> list[dict]:
    rows = []
    base_time = datetime.now(timezone.utc) - timedelta(days=90)
    for _ in range(n):
        hotel_name, country = random.choice(HOTELS)
        check_in = base_time + timedelta(days=random.randint(1, 120))
        nights = random.randint(1, 7)
        rows.append(
            {
                "booking_id": str(uuid.uuid4()),
                "event_timestamp": base_time + timedelta(seconds=random.randint(0, 90 * 86400)),
                "user_id": str(uuid.uuid4()),
                "hotel_name": hotel_name,
                "room_type": random.choice(ROOM_TYPES),
                "check_in_date": check_in.date(),
                "check_out_date": (check_in + timedelta(days=nights)).date(),
                "total_price_usd": round(random.uniform(60, 1200), 2),
                "payment_method": random.choice(PAYMENT_METHODS),
                "country": country,
            }
        )
    return rows


def generate_impressions(n: int) -> list[dict]:
    rows = []
    base_time = datetime.now(timezone.utc) - timedelta(days=90)
    for _ in range(n):
        partner_id, partner_name = random.choice(PARTNERS)
        hotel_name, country = random.choice(HOTELS)
        rows.append(
            {
                "impression_id": str(uuid.uuid4()),
                "event_timestamp": base_time + timedelta(seconds=random.randint(0, 90 * 86400)),
                "partner_id": partner_id,
                "partner_name": partner_name,
                "placement": random.choice(PLACEMENTS),
                "hotel_name": hotel_name,
                "country": country,
                "bid_amount_usd": round(random.uniform(0.05, 5.00), 4),
            }
        )
    return rows


def generate_clicks(impressions: list[dict], bookings: list[dict], click_rate: float = 0.03) -> list[dict]:
    """Generate clicks for a subset of impressions. Some clicks lead to bookings."""
    clicked = random.sample(impressions, k=int(len(impressions) * click_rate))
    booking_ids = [b["booking_id"] for b in bookings]
    rows = []
    for imp in clicked:
        # ~20% of clicks result in a booking
        booking_id = random.choice(booking_ids) if random.random() < 0.20 else None
        rows.append(
            {
                "click_id": str(uuid.uuid4()),
                "impression_id": imp["impression_id"],
                "event_timestamp": imp["event_timestamp"] + timedelta(seconds=random.randint(1, 300)),
                "user_id": str(uuid.uuid4()),
                "booking_id": booking_id,
            }
        )
    return rows


# --- Database ---


def insert_bookings(cur, rows: list[dict]):
    cur.executemany(
        """
        INSERT INTO raw.bookings
            (booking_id, event_timestamp, user_id, hotel_name, room_type,
             check_in_date, check_out_date, total_price_usd, payment_method, country)
        VALUES
            (%(booking_id)s, %(event_timestamp)s, %(user_id)s, %(hotel_name)s, %(room_type)s,
             %(check_in_date)s, %(check_out_date)s, %(total_price_usd)s, %(payment_method)s, %(country)s)
        ON CONFLICT (booking_id) DO NOTHING
        """,
        rows,
    )


def insert_impressions(cur, rows: list[dict]):
    cur.executemany(
        """
        INSERT INTO raw.ad_impressions
            (impression_id, event_timestamp, partner_id, partner_name, placement,
             hotel_name, country, bid_amount_usd)
        VALUES
            (%(impression_id)s, %(event_timestamp)s, %(partner_id)s, %(partner_name)s, %(placement)s,
             %(hotel_name)s, %(country)s, %(bid_amount_usd)s)
        ON CONFLICT (impression_id) DO NOTHING
        """,
        rows,
    )


def insert_clicks(cur, rows: list[dict]):
    cur.executemany(
        """
        INSERT INTO raw.ad_clicks
            (click_id, impression_id, event_timestamp, user_id, booking_id)
        VALUES
            (%(click_id)s, %(impression_id)s, %(event_timestamp)s, %(user_id)s, %(booking_id)s)
        ON CONFLICT (click_id) DO NOTHING
        """,
        rows,
    )


def main():
    parser = argparse.ArgumentParser(description="Seed raw tables with fake data")
    parser.add_argument("--bookings", type=int, default=10_000, help="Number of bookings to generate")
    parser.add_argument("--impressions", type=int, default=50_000, help="Number of ad impressions to generate")
    parser.add_argument("--click-rate", type=float, default=0.03, help="Fraction of impressions that get clicked")
    args = parser.parse_args()

    print(f"Generating {args.bookings} bookings...")
    bookings = generate_bookings(args.bookings)

    print(f"Generating {args.impressions} ad impressions...")
    impressions = generate_impressions(args.impressions)

    click_count = int(args.impressions * args.click_rate)
    print(f"Generating ~{click_count} ad clicks ({args.click_rate:.0%} CTR)...")
    clicks = generate_clicks(impressions, bookings, args.click_rate)

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="booking_analytics",
        user="booking_user",
        password="booking_pass",
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("Inserting bookings...")
        insert_bookings(cur, bookings)

        print("Inserting impressions...")
        insert_impressions(cur, impressions)

        print("Inserting clicks...")
        insert_clicks(cur, clicks)

        conn.commit()
        print(f"\nDone! Seeded {len(bookings)} bookings, {len(impressions)} impressions, {len(clicks)} clicks.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
