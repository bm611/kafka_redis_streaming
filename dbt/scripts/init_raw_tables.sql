-- Raw schema: landing zone for source data
CREATE SCHEMA IF NOT EXISTS raw;

-- Booking events (mirrors the Kafka project's schema)
CREATE TABLE IF NOT EXISTS raw.bookings (
    booking_id      TEXT PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    hotel_name      TEXT NOT NULL,
    room_type       TEXT NOT NULL,
    check_in_date   DATE NOT NULL,
    check_out_date  DATE NOT NULL,
    total_price_usd NUMERIC(10, 2) NOT NULL,
    payment_method  TEXT NOT NULL,
    country         TEXT NOT NULL,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Ad impressions from third-party providers
CREATE TABLE IF NOT EXISTS raw.ad_impressions (
    impression_id   TEXT PRIMARY KEY,
    event_timestamp TIMESTAMPTZ NOT NULL,
    partner_id      TEXT NOT NULL,
    partner_name    TEXT NOT NULL,
    placement       TEXT NOT NULL,       -- e.g. 'search_results', 'hotel_page', 'checkout'
    hotel_name      TEXT NOT NULL,
    country         TEXT NOT NULL,
    bid_amount_usd  NUMERIC(10, 4) NOT NULL,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Ad clicks (subset of impressions that were clicked)
CREATE TABLE IF NOT EXISTS raw.ad_clicks (
    click_id        TEXT PRIMARY KEY,
    impression_id   TEXT NOT NULL REFERENCES raw.ad_impressions(impression_id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    booking_id      TEXT,                -- NULL if click didn't lead to a booking
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common join patterns
CREATE INDEX IF NOT EXISTS idx_bookings_hotel ON raw.bookings(hotel_name);
CREATE INDEX IF NOT EXISTS idx_impressions_partner ON raw.ad_impressions(partner_id);
CREATE INDEX IF NOT EXISTS idx_impressions_hotel ON raw.ad_impressions(hotel_name);
CREATE INDEX IF NOT EXISTS idx_clicks_impression ON raw.ad_clicks(impression_id);
CREATE INDEX IF NOT EXISTS idx_clicks_booking ON raw.ad_clicks(booking_id);
