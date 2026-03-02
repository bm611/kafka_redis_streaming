from kafka import KafkaConsumer
import json
import sqlite3
from datetime import datetime, timezone

from schema import validate_booking_event
from dlq import send_to_dlq

CONSUMER_NAME = "warehouse-service"
BATCH_SIZE = 10
DB_PATH = "bookings_warehouse.db"

def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id TEXT PRIMARY KEY,
            event_timestamp TEXT NOT NULL,
            user_id TEXT NOT NULL,
            hotel_name TEXT NOT NULL,
            room_type TEXT NOT NULL,
            check_in_date TEXT NOT NULL,
            check_out_date TEXT NOT NULL,
            total_price_usd REAL NOT NULL,
            payment_method TEXT NOT NULL,
            country_of_user TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS daily_hotel_stats (
            stat_date TEXT NOT NULL,
            hotel_name TEXT NOT NULL,
            total_bookings INTEGER NOT NULL,
            total_revenue REAL NOT NULL,
            avg_price REAL NOT NULL,
            max_price REAL NOT NULL,
            PRIMARY KEY (stat_date, hotel_name)
        );

        CREATE TABLE IF NOT EXISTS payment_method_stats (
            stat_date TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            booking_count INTEGER NOT NULL,
            total_revenue REAL NOT NULL,
            PRIMARY KEY (stat_date, payment_method)
        );

        CREATE TABLE IF NOT EXISTS country_stats (
            stat_date TEXT NOT NULL,
            country TEXT NOT NULL,
            booking_count INTEGER NOT NULL,
            total_revenue REAL NOT NULL,
            PRIMARY KEY (stat_date, country)
        );
    """)
    conn.commit()

def flush_batch(conn, batch):
    now = datetime.now(timezone.utc).isoformat()

    # INSERT OR IGNORE ensures idempotency — duplicate booking_ids are skipped
    conn.executemany(
        """INSERT OR IGNORE INTO bookings
           (booking_id, event_timestamp, user_id, hotel_name, room_type,
            check_in_date, check_out_date, total_price_usd, payment_method,
            country_of_user, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [(
            b['booking_id'], b['timestamp'], b['user_id'], b['hotel_name'],
            b['room_type'], b['check_in_date'], b['check_out_date'],
            b['total_price_usd'], b['payment_method'], b['country_of_user'], now
        ) for b in batch]
    )

    # Aggregates are recomputed from the deduplicated bookings table,
    # so duplicate Kafka deliveries cannot skew the stats
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    conn.execute("DELETE FROM daily_hotel_stats WHERE stat_date = ?", (today,))
    conn.execute("""
        INSERT INTO daily_hotel_stats (stat_date, hotel_name, total_bookings, total_revenue, avg_price, max_price)
        SELECT date(event_timestamp), hotel_name,
               COUNT(*), SUM(total_price_usd), AVG(total_price_usd), MAX(total_price_usd)
        FROM bookings
        WHERE date(event_timestamp) = ?
        GROUP BY hotel_name
    """, (today,))

    conn.execute("DELETE FROM payment_method_stats WHERE stat_date = ?", (today,))
    conn.execute("""
        INSERT INTO payment_method_stats (stat_date, payment_method, booking_count, total_revenue)
        SELECT date(event_timestamp), payment_method, COUNT(*), SUM(total_price_usd)
        FROM bookings
        WHERE date(event_timestamp) = ?
        GROUP BY payment_method
    """, (today,))

    conn.execute("DELETE FROM country_stats WHERE stat_date = ?", (today,))
    conn.execute("""
        INSERT INTO country_stats (stat_date, country, booking_count, total_revenue)
        SELECT date(event_timestamp), country_of_user, COUNT(*), SUM(total_price_usd)
        FROM bookings
        WHERE date(event_timestamp) = ?
        GROUP BY country_of_user
    """, (today,))

    conn.commit()

consumer = KafkaConsumer(
    'booking-events',
    bootstrap_servers='localhost:9092',
    group_id=CONSUMER_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

conn = sqlite3.connect(DB_PATH)
init_db(conn)

batch = []
print(f"Warehouse Service listening (batch size={BATCH_SIZE})...")
for msg in consumer:
    try:
        b = msg.value

        errors = validate_booking_event(b)
        if errors:
            send_to_dlq(b, f"schema validation failed: {errors}", CONSUMER_NAME)
            continue

        batch.append(b)
        print(f"  [BUFFER] {len(batch)}/{BATCH_SIZE} — {b['booking_id'][:8]}... | {b['hotel_name']}")

        if len(batch) >= BATCH_SIZE:
            flush_batch(conn, batch)
            total = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
            print(f"  [FLUSHED] {len(batch)} events written | {total} total bookings in warehouse")
            batch = []

    except Exception as e:
        send_to_dlq(b if 'b' in dir() else msg.value, str(e), CONSUMER_NAME)
