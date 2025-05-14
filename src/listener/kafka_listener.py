"""
Kafka Signal Listener

This module provides functionality to listen for signals from Kafka topics.
"""

import logging
import json
import threading
from typing import Dict, Any, List, Optional, Union

from .signal_listener import SignalListener

logger = logging.getLogger(__name__)

# Import kafka conditionally to avoid hard dependency
try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
    KAFKA_AVAILABLE = True
except ImportError:
    logger.warning("confluent-kafka package not found. KafkaListener will be non-functional.")
    KAFKA_AVAILABLE = False


class KafkaListener(SignalListener):
    """Listener for Kafka topic signals."""
    
    def __init__(self, 
                 name: str = "kafka_listener",
                 bootstrap_servers: Union[str, List[str]] = "localhost:9092",
                 topics: List[str] = None,
                 group_id: str = "signal_collector",
                 auto_offset_reset: str = "latest",
                 consumer_timeout_ms: int = 1000,
                 **consumer_config):
        """Initialize Kafka listener.
        
        Args:
            name: Unique name for this listener
            bootstrap_servers: Kafka bootstrap servers (string or list)
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            auto_offset_reset: Where to start consuming ('earliest' or 'latest')
            consumer_timeout_ms: Consumer timeout in milliseconds
            **consumer_config: Additional Kafka consumer configuration
        """
        super().__init__(name)
        
        if not KAFKA_AVAILABLE:
            logger.error("Cannot initialize KafkaListener: confluent-kafka package not installed")
            return
            
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or []
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer_timeout_ms = consumer_timeout_ms
        self.consumer_config = consumer_config
        self._consumer = None
        self._consumer_lock = threading.Lock()
        
        logger.info(f"Initialized KafkaListener for topics: {', '.join(self.topics)}")
    
    def _create_consumer(self) -> Optional[Consumer]:
        """Create and configure a Kafka consumer.
        
        Returns:
            Configured Kafka consumer or None if creation failed
        """
        if not KAFKA_AVAILABLE:
            return None
            
        try:
            # Base configuration
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': self.auto_offset_reset,
                'enable.auto.commit': True,
                'session.timeout.ms': 30000,
                'max.poll.interval.ms': 300000,
            }
            
            # Add any additional configuration
            config.update(self.consumer_config)
            
            # Create consumer
            consumer = Consumer(config)
            
            # Subscribe to topics
            if self.topics:
                consumer.subscribe(self.topics)
                logger.info(f"Subscribed to Kafka topics: {', '.join(self.topics)}")
            else:
                logger.warning("No Kafka topics specified for subscription")
                
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return None
    
    def add_topics(self, topics: List[str]) -> None:
        """Add topics to the subscription list.
        
        Args:
            topics: List of topic names to add
        """
        with self._consumer_lock:
            new_topics = [topic for topic in topics if topic not in self.topics]
            if not new_topics:
                return
                
            self.topics.extend(new_topics)
            
            if self._consumer and self.is_running:
                self._consumer.subscribe(self.topics)
                logger.info(f"Added topics to subscription: {', '.join(new_topics)}")
    
    def remove_topics(self, topics: List[str]) -> None:
        """Remove topics from the subscription list.
        
        Args:
            topics: List of topic names to remove
        """
        with self._consumer_lock:
            for topic in topics:
                if topic in self.topics:
                    self.topics.remove(topic)
            
            if self._consumer and self.is_running:
                self._consumer.subscribe(self.topics)
                logger.info(f"Updated topic subscription after removal: {', '.join(self.topics)}")
    
    def _listen_loop(self) -> None:
        """Main listening loop for Kafka signals."""
        if not KAFKA_AVAILABLE:
            logger.error(f"{self.name} cannot start: confluent-kafka package not installed")
            return
            
        logger.info(f"{self.name} listening loop started")
        
        with self._consumer_lock:
            self._consumer = self._create_consumer()
            if not self._consumer:
                logger.error(f"{self.name} failed to create consumer")
                return
        
        try:
            while self.is_running:
                try:
                    # Poll for messages
                    msg = self._consumer.poll(self.consumer_timeout_ms / 1000.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event - not an error
                            continue
                        else:
                            # Log the error
                            logger.error(f"Kafka error: {msg.error()}")
                            continue
                    
                    # Process the message
                    try:
                        # Try to parse as JSON
                        value = msg.value().decode('utf-8')
                        data = json.loads(value)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # If not valid JSON, use raw value
                        data = {'raw': msg.value()}
                    
                    # Create signal data
                    signal_data = {
                        'source': 'kafka',
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'timestamp': msg.timestamp()[1] if msg.timestamp()[0] else None,
                        'key': msg.key().decode('utf-8') if msg.key() else None,
                        'data': data
                    }
                    
                    # Notify callbacks
                    self._notify_callbacks(signal_data)
                    
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    
        except Exception as e:
            logger.error(f"Error in Kafka listening loop: {e}")
            
        finally:
            # Clean up
            with self._consumer_lock:
                if self._consumer:
                    try:
                        self._consumer.close()
                        logger.info(f"{self.name} consumer closed")
                    except Exception as e:
                        logger.error(f"Error closing Kafka consumer: {e}")
                    self._consumer = None
