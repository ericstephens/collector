"""
Signal Collector

This module demonstrates how to use the signal listeners to collect signals from various sources.
"""

import logging
import json
import time
import argparse
import sys
from typing import Dict, Any, List

# Import signal listeners
from .listener.signal_listener import SignalListenerManager
from .listener.teams_listener import TeamsListener
from .listener.kafka_listener import KafkaListener
from .listener.datadog_listener import DataDogListener
from .listener.controlm_listener import ControlMListener

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('signal_collector.log')
    ]
)
logger = logging.getLogger(__name__)


class SignalCollector:
    """Main class for collecting signals from various sources."""
    
    def __init__(self, config_file: str = None):
        """Initialize the signal collector.
        
        Args:
            config_file: Path to configuration file (JSON)
        """
        self.config = self._load_config(config_file)
        self.manager = SignalListenerManager()
        self.signals = []
        self.max_signals = 1000  # Maximum number of signals to keep in memory
        
        logger.info("Signal Collector initialized")
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from a JSON file.
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        default_config = {
            "teams": {
                "enabled": False,
                "tenant_id": "",
                "client_id": "",
                "client_secret": "",
                "poll_interval": 30.0
            },
            "kafka": {
                "enabled": False,
                "bootstrap_servers": "localhost:9092",
                "topics": ["signals"],
                "group_id": "signal_collector"
            },
            "datadog": {
                "enabled": False,
                "api_key": "",
                "app_key": "",
                "site": "datadoghq.com",
                "poll_interval": 60.0,
                "metrics": [],
                "monitors": []
            },
            "controlm": {
                "enabled": False,
                "endpoint": "http://localhost:8080/api",
                "username": "",
                "password": "",
                "poll_interval": 60.0
            }
        }
        
        if not config_file:
            logger.warning("No configuration file provided, using defaults")
            return default_config
            
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                
            # Merge with defaults
            for section in default_config:
                if section not in config:
                    config[section] = default_config[section]
                else:
                    for key in default_config[section]:
                        if key not in config[section]:
                            config[section][key] = default_config[section][key]
                            
            logger.info(f"Loaded configuration from {config_file}")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return default_config
    
    def setup_listeners(self) -> None:
        """Set up signal listeners based on configuration."""
        # Microsoft Teams listener
        if self.config["teams"]["enabled"]:
            teams_listener = TeamsListener(
                name="teams_listener",
                tenant_id=self.config["teams"]["tenant_id"],
                client_id=self.config["teams"]["client_id"],
                client_secret=self.config["teams"]["client_secret"],
                poll_interval=self.config["teams"]["poll_interval"]
            )
            self.manager.add_listener(teams_listener)
            logger.info("Added Microsoft Teams listener")
        
        # Kafka listener
        if self.config["kafka"]["enabled"]:
            kafka_listener = KafkaListener(
                name="kafka_listener",
                bootstrap_servers=self.config["kafka"]["bootstrap_servers"],
                topics=self.config["kafka"]["topics"],
                group_id=self.config["kafka"]["group_id"]
            )
            self.manager.add_listener(kafka_listener)
            logger.info("Added Kafka listener")
        
        # DataDog listener
        if self.config["datadog"]["enabled"]:
            datadog_listener = DataDogListener(
                name="datadog_listener",
                api_key=self.config["datadog"]["api_key"],
                app_key=self.config["datadog"]["app_key"],
                site=self.config["datadog"]["site"],
                poll_interval=self.config["datadog"]["poll_interval"],
                metrics=self.config["datadog"]["metrics"],
                monitors=self.config["datadog"]["monitors"]
            )
            self.manager.add_listener(datadog_listener)
            logger.info("Added DataDog listener")
        
        # Control-M listener
        if self.config["controlm"]["enabled"]:
            controlm_listener = ControlMListener(
                name="controlm_listener",
                endpoint=self.config["controlm"]["endpoint"],
                username=self.config["controlm"]["username"],
                password=self.config["controlm"]["password"],
                poll_interval=self.config["controlm"]["poll_interval"]
            )
            self.manager.add_listener(controlm_listener)
            logger.info("Added Control-M listener")
        
        # Register global callback for all listeners
        self.manager.register_global_callback(self.signal_callback)
    
    def signal_callback(self, signal_data: Dict[str, Any]) -> None:
        """Callback function for processing signals.
        
        Args:
            signal_data: Signal data dictionary
        """
        # Add timestamp if not present
        if "timestamp" not in signal_data:
            signal_data["timestamp"] = time.time()
            
        # Add to signals list
        self.signals.append(signal_data)
        
        # Trim signals list if it gets too large
        if len(self.signals) > self.max_signals:
            self.signals = self.signals[-self.max_signals:]
            
        # Log signal receipt
        source = signal_data.get("source", "unknown")
        logger.info(f"Received signal from {source}")
        
        # Process signal (example: just print to console)
        print(f"Signal from {source}: {json.dumps(signal_data, indent=2)}")
    
    def start(self) -> None:
        """Start all signal listeners."""
        if not self.manager.listeners:
            logger.warning("No listeners configured")
            return
            
        self.manager.start_all()
        logger.info("Started all signal listeners")
    
    def stop(self) -> None:
        """Stop all signal listeners."""
        self.manager.stop_all()
        logger.info("Stopped all signal listeners")
    
    def run(self) -> None:
        """Run the signal collector."""
        try:
            self.setup_listeners()
            self.start()
            
            logger.info("Signal Collector running. Press Ctrl+C to stop.")
            
            # Keep running until interrupted
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Signal Collector interrupted")
        finally:
            self.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Signal Collector")
    parser.add_argument("--config", "-c", help="Path to configuration file")
    args = parser.parse_args()
    
    collector = SignalCollector(args.config)
    collector.run()


if __name__ == "__main__":
    main()
