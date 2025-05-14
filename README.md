# Signal Collector

A Python utility for listening to signals from multiple sources:
- Microsoft Teams
- Kafka
- DataDog (via OpenTelemetry)
- Control-M

## Project Structure

```
collector/
├── collector                # Management script for start/stop/restart
├── config.json              # Configuration file
├── environment.yaml          # Conda environment configuration
├── spec.md                  # Project specifications
├── src/
│   ├── signal_collector.py    # Main signal collector implementation
│   └── listener/             # Signal listener modules
│       ├── __init__.py
│       ├── signal_listener.py    # Base signal listener
│       ├── teams_listener.py     # Microsoft Teams listener
│       ├── kafka_listener.py     # Kafka listener
│       ├── datadog_listener.py   # DataDog listener
│       └── controlm_listener.py  # Control-M listener
└── tests/
    ├── api/                 # API tests
    ├── db/                  # Database tests
    └── frontend/            # Frontend tests
```

## Setup

### Environment Setup

According to project specifications, this project uses conda for Python environments. An `environment.yaml` file is provided for easy setup:

```bash
# Create conda environment using the environment.yaml file
conda env create -f environment.yaml

# Activate the collector environment
conda activate collector
```

The environment includes all necessary dependencies:
- Python 3.13
- requests (for API calls)
- confluent-kafka (for Kafka listener)
- opentelemetry packages (for DataDog monitoring)
- pytest (for API unit tests)

If you prefer to install packages manually instead of using the environment.yaml file:

```bash
# Create conda environment with the same name as the project
conda create -n collector python=3.13
conda activate collector

# Install required packages
conda install -c conda-forge requests pytest
pip install confluent-kafka opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp
```

## Configuration

Edit the `config.json` file to configure the signal listeners:

```json
{
  "teams": {
    "enabled": true,
    "tenant_id": "your_tenant_id",
    "client_id": "your_client_id",
    "client_secret": "your_client_secret",
    "poll_interval": 30.0
  },
  "kafka": {
    "enabled": true,
    "bootstrap_servers": "localhost:9092",
    "topics": ["signals", "events", "alerts"],
    "group_id": "signal_collector"
  },
  ...
}
```

## Usage

Run the signal collector with:

```bash
# Activate conda environment
conda activate collector

# Run with default config
python -m src.signal_collector

# Run with custom config
python -m src.signal_collector --config path/to/config.json
```

## Signal Sources

### Microsoft Teams

The Teams listener uses the Microsoft Graph API to listen for signals from Microsoft Teams. You'll need to register an application in the Azure portal and obtain the tenant ID, client ID, and client secret.

### Kafka

The Kafka listener uses the confluent-kafka package to consume messages from Kafka topics. You'll need to specify the bootstrap servers, topics, and consumer group ID.

### DataDog

The DataDog listener uses the DataDog API and OpenTelemetry to monitor metrics and alerts. You'll need to provide your DataDog API key and application key.

### Control-M

The Control-M listener is a stub implementation that can be extended when the Control-M API specifications are available. It currently simulates job and alert data.

## Extending

To add a new signal source:

1. Create a new listener class that inherits from `SignalListener`
2. Implement the `_listen_loop` method to fetch signals from the source
3. Add the listener to the `SignalCollector` class in `signal_collector.py`
4. Update the configuration schema in `signal_collector.py` and `config.json`

## Management

As per project specifications, all stop, start, restart operations should use the management script. The project includes a `collector` script that handles these operations:

```bash
# Start the collector
./collector start

# Stop the collector
./collector stop

# Restart the collector
./collector restart

# Check the status of the collector
./collector status
```

The script automatically activates the conda environment and manages the process using a PID file.

## Testing

Tests should be added to the appropriate test directories:
- `tests/api/` for API tests
- `tests/db/` for database tests
- `tests/frontend/` for frontend tests

Use pytest for Python unit tests.
