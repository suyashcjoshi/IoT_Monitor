-- Initialize Database for Data Center Monitoring
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create schema for data center monitoring
CREATE SCHEMA IF NOT EXISTS datacenter;

-- Sensor data table (from Kafka/Telegraf)
CREATE TABLE IF NOT EXISTS datacenter.sensor_readings (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    device_id VARCHAR(100) NOT NULL,
    device_type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    rack_id VARCHAR(50),
    measurement_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(20),
    status VARCHAR(20) DEFAULT 'active',
    metadata JSONB
);

-- Weather data table (from Open-Meteo API via Airbyte)
CREATE TABLE IF NOT EXISTS datacenter.weather_data (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    location VARCHAR(100) NOT NULL DEFAULT 'London',
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    weather_code INTEGER,
    source VARCHAR(50) DEFAULT 'open-meteo'
);

-- Cooling system metrics
CREATE TABLE IF NOT EXISTS datacenter.cooling_systems (
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    system_id VARCHAR(100) NOT NULL,
    system_type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    temperature_setpoint DOUBLE PRECISION,
    actual_temperature DOUBLE PRECISION,
    efficiency_percent DOUBLE PRECISION,
    power_consumption_watts DOUBLE PRECISION,
    status VARCHAR(20) DEFAULT 'running'
);

-- Convert to TimescaleDB hypertables for better time-series performance
SELECT create_hypertable('datacenter.sensor_readings', 'time', if_not_exists => TRUE);
SELECT create_hypertable('datacenter.weather_data', 'time', if_not_exists => TRUE);
SELECT create_hypertable('datacenter.cooling_systems', 'time', if_not_exists => TRUE);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sensor_device_time ON datacenter.sensor_readings (device_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_type_time ON datacenter.sensor_readings (measurement_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_location_time ON datacenter.sensor_readings (location, time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_time ON datacenter.weather_data (time DESC);
CREATE INDEX IF NOT EXISTS idx_cooling_system_time ON datacenter.cooling_systems (system_id, time DESC);

-- Create views for easier querying
CREATE OR REPLACE VIEW datacenter.latest_sensor_readings AS
SELECT DISTINCT ON (device_id, measurement_type)
    device_id,
    device_type,
    location,
    rack_id,
    measurement_type,
    value,
    unit,
    status,
    time
FROM datacenter.sensor_readings
ORDER BY device_id, measurement_type, time DESC;

CREATE OR REPLACE VIEW datacenter.current_room_temperature AS
SELECT 
    location,
    AVG(value) as avg_temperature,
    MIN(value) as min_temperature,
    MAX(value) as max_temperature,
    COUNT(*) as sensor_count,
    MAX(time) as last_updated
FROM datacenter.sensor_readings 
WHERE measurement_type = 'temperature' 
    AND time > NOW() - INTERVAL '5 minutes'
GROUP BY location;

-- Sample data for testing (will be replaced by real data from Airbyte)
INSERT INTO datacenter.sensor_readings (device_id, device_type, location, rack_id, measurement_type, value, unit) VALUES
('temp_001', 'temperature_sensor', 'Server Room A', 'rack-a1', 'temperature', 23.5, 'celsius'),
('temp_002', 'temperature_sensor', 'Server Room A', 'rack-a2', 'temperature', 24.1, 'celsius'),
('hum_001', 'humidity_sensor', 'Server Room A', 'rack-a1', 'humidity', 45.2, 'percent'),
('temp_003', 'temperature_sensor', 'Server Room B', 'rack-b1', 'temperature', 22.8, 'celsius');

INSERT INTO datacenter.cooling_systems (system_id, system_type, location, temperature_setpoint, actual_temperature, efficiency_percent, power_consumption_watts) VALUES
('hvac_001', 'primary_cooling', 'Server Room A', 22.0, 23.5, 87.5, 15500),
('hvac_002', 'secondary_cooling', 'Server Room B', 23.0, 22.8, 92.1, 12300);

-- Grant permissions for Airbyte user (we'll create this user)
CREATE USER airbyte_user WITH PASSWORD 'airbyte_pass';
GRANT USAGE ON SCHEMA datacenter TO airbyte_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA datacenter TO airbyte_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA datacenter TO airbyte_user;

-- Also grant to admin for Grafana access
GRANT USAGE ON SCHEMA datacenter TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA datacenter TO admin;
