import sqlite3

DB_PATH = "data/bookings_warehouse.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 60)
total = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
print(f"Total bookings in warehouse: {total}\n")

print("--- Revenue by Hotel (today) ---")
for row in conn.execute("SELECT * FROM daily_hotel_stats ORDER BY total_revenue DESC"):
    print(f"  {row['hotel_name']:30s} | {row['total_bookings']:3d} bookings | ${row['total_revenue']:,.0f} revenue | avg ${row['avg_price']:,.0f}")

print("\n--- Bookings by Payment Method (today) ---")
for row in conn.execute("SELECT * FROM payment_method_stats ORDER BY booking_count DESC"):
    print(f"  {row['payment_method']:15s} | {row['booking_count']:3d} bookings | ${row['total_revenue']:,.0f}")

print("\n--- Top 10 Countries (today) ---")
for row in conn.execute("SELECT * FROM country_stats ORDER BY booking_count DESC LIMIT 10"):
    print(f"  {row['country']:30s} | {row['booking_count']:3d} bookings | ${row['total_revenue']:,.0f}")

print("=" * 60)
