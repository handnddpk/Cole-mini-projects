-- Initialize PostgreSQL database for IoT analytics

-- Create database (if not exists)
CREATE DATABASE IF NOT EXISTS iot_analytics;

-- Use the database
\c iot_analytics;

-- Create tables for IoT data storage
CREATE TABLE IF NOT EXISTS device_metadata (
    device_id VARCHAR(50) PRIMARY KEY,
    device_type VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    installation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES device_metadata(device_id),
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(8,2),
    battery_level INTEGER,
    signal_strength INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_alerts (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES device_metadata(device_id),
    alert_type VARCHAR(50) NOT NULL,
    message TEXT,
    severity VARCHAR(20) DEFAULT 'medium',
    timestamp TIMESTAMP NOT NULL,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_aggregates (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES device_metadata(device_id),
    date DATE NOT NULL,
    avg_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    avg_humidity DECIMAL(5,2),
    avg_pressure DECIMAL(8,2),
    total_readings INTEGER,
    alerts_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, date)
);

CREATE TABLE IF NOT EXISTS ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES device_metadata(device_id),
    prediction_type VARCHAR(50) NOT NULL,
    predicted_value DECIMAL(10,4),
    confidence DECIMAL(5,4),
    model_version VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomaly_detections (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES device_metadata(device_id),
    anomaly_score DECIMAL(8,6),
    anomaly_type VARCHAR(50),
    detected_at TIMESTAMP NOT NULL,
    features JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_timestamp ON sensor_readings(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_device_alerts_device_timestamp ON device_alerts(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_daily_aggregates_device_date ON daily_aggregates(device_id, date);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_device_timestamp ON ml_predictions(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_detections_device_timestamp ON anomaly_detections(device_id, detected_at);

-- Insert sample device metadata
INSERT INTO device_metadata (device_id, device_type, location) VALUES
('device_001', 'temperature_sensor', 'Building A - Floor 1'),
('device_002', 'humidity_sensor', 'Building A - Floor 2'),
('device_003', 'pressure_sensor', 'Building B - Floor 1'),
('device_004', 'multi_sensor', 'Building B - Floor 2'),
('device_005', 'environmental_sensor', 'Outdoor Station 1')
ON CONFLICT (device_id) DO NOTHING;

-- Create views for analytics
CREATE OR REPLACE VIEW device_health_summary AS
SELECT 
    dm.device_id,
    dm.device_type,
    dm.location,
    dm.status,
    COUNT(sr.id) as total_readings,
    MAX(sr.timestamp) as last_reading,
    AVG(sr.battery_level) as avg_battery_level,
    COUNT(da.id) as alert_count
FROM device_metadata dm
LEFT JOIN sensor_readings sr ON dm.device_id = sr.device_id
LEFT JOIN device_alerts da ON dm.device_id = da.device_id AND da.resolved = FALSE
GROUP BY dm.device_id, dm.device_type, dm.location, dm.status;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
