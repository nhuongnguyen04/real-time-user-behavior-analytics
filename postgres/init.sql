-- Bảng người dùng và sự kiện
CREATE TABLE user_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    action VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    device_id VARCHAR(50),
    device_type VARCHAR(50),
    location VARCHAR(100),
    user_segment VARCHAR(50),
    ip_address VARCHAR(50),
    UNIQUE (user_id, timestamp, action, device_id)
);

-- Bảng thống kê hoạt động người dùng
CREATE TABLE anomalous_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    window_start VARCHAR(50) NOT NULL,
    window_end VARCHAR(50) NOT NULL,
    event_count INTEGER NOT NULL,
    detected_at TIMESTAMP DEFAULT NOW(),
    detector_type VARCHAR(50) DEFAULT 'zscore',
    severity VARCHAR(20),
    UNIQUE (user_id, window_start, window_end)
);
-- Bảng phát hiện bất thường (spam / click nhiều)
CREATE TABLE user_activity_summary (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    UNIQUE (user_id, window_start, window_end)
);

-- Bảng users cho enrich
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE,
    name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Indexes for performance optimization
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(timestamp);

CREATE INDEX IF NOT EXISTS idx_anomalous_events_detected_at
    ON anomalous_events (detected_at);

CREATE INDEX IF NOT EXISTS idx_anomalous_events_detector_type
    ON anomalous_events (detector_type);
