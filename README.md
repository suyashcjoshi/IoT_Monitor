# Data Center IoT Monitoring Pipeline

A real-time data engineering pipeline demonstrating IoT sensor data collection and analytics using modern cloud tools.

## Architecture

```
┌─────────────┐    ┌───────┐    ┌─────────┐    ┌──────────┐    ┌────────────┐
│ IoT Sensors │───▶│ Kafka │───▶│ REST API│───▶│ Airbyte  │───▶│ MotherDuck │
│ (Simulator) │    │       │    │ Server  │    │ Cloud    │    │  (DuckDB)  │
└─────────────┘    └───────┘    └─────────┘    └──────────┘    └────────────┘
```

## Purpose

Demonstrates modern data engineering practices by building a scalable IoT pipeline. 
Simulates data center monitoring where facilities spend 30-40% of energy on cooling.

## Technology Stack

- **Data Generation**: Python sensor simulator
- **Streaming**: Apache Kafka  
- **API**: Flask REST server
- **ETL**: Airbyte Cloud
- **Database**: MotherDuck (Cloud DuckDB)
- **Infrastructure**: Docker, ngrok

## Quick Start

### 1. Setup Local Environment
```bash
git clone <repo>
cd datacenter-monitoring
python3 -m venv venv
source venv/bin/activate
pip install flask kafka-python
```

### 2. Start Services
```bash
docker-compose up -d
python api_server.py
```

### 3. Expose to Internet
```bash
ngrok http 5001
# Note the HTTPS URL
```

### 4. Configure Pipeline
1. Create MotherDuck database: `iot-metrics`
2. Setup Airbyte Cloud source: HTTP API pointing to `https://your-ngrok-url.ngrok.io/api/airbyte/all`
3. Setup Airbyte destination: MotherDuck
4. Create connection with hourly sync

## Data Schema

**Sensor Data**
```json
{
  "source_type": "sensor",
  "device_id": "temp_rack-a1", 
  "location": "Server Room A",
  "measurement_type": "temperature",
  "value": 23.5,
  "timestamp": "2025-08-21T16:48:00Z"
}
```

**Cooling System Data**
```json
{
  "source_type": "cooling",
  "system_id": "hvac_primary_a",
  "efficiency_percent": 89.2,
  "power_consumption_watts": 16800,
  "timestamp": "2025-08-21T16:48:00Z"
}
```

## Querying Data

```sql
-- Check data ingestion
SELECT COUNT(*) FROM "_airbyte_raw_HTTP";
```

```sql
SELECT * FROM "_airbyte_raw_HTTP" LIMIT 3;
```

```sql
_airbyte_emitted_at	record_id	source_type	device_id	sensor_value	sensor_timestamp
2025-08-21 16:56:13.921146					"""2025-08-21T16:50:39.313663+00:00"""
2025-08-21 16:56:13.921301					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.92139					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921471					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921535					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921583					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921627					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.92167					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921713					"""2025-08-21T16:51:09.315307+00:00"""
2025-08-21 16:56:13.921766					"""2025-08-21T16:51:09.315307+00:00"""
```


## Real-World Extensions

**Additional Sources**
- Weather APIs for outdoor correlation
- Network monitoring via SNMP
- Application logs and metrics
- Security event streams

**Production Features**
- Authentication and rate limiting
- Data validation and error handling
- Kubernetes deployment
- Monitoring and alerting
- Machine learning for anomaly detection

**Industry Applications**
- Manufacturing equipment monitoring
- Smart city environmental tracking
- Healthcare patient monitoring
- Financial transaction analysis

## Business Value

- Real-time visibility into infrastructure
- Energy cost optimization opportunities
- Predictive maintenance capabilities
- Compliance and audit reporting
- Scalable foundation for additional sensors

## Skills Demonstrated

- End-to-end data pipeline design
- Real-time stream processing
- Cloud-native architecture
- REST API development
- Modern ETL practices
- SQL analytics on JSON data

## Project Structure

```
datacenter-monitoring/
├── docker-compose.yml     # Infrastructure
├── api_server.py         # REST API
├── datacenter-simulator/ # IoT simulator
├── telegraf/            # System metrics
└── README.md           # Documentation
```

This pipeline demonstrates production-ready patterns for IoT data engineering using industry-standard tools and cloud platforms.
