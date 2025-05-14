"""
Signal Listener Utility

This module provides classes for listening to signals from various sources:
- Microsoft Teams
- Kafka
- DataDog (via OpenTelemetry)
- Control-M
"""

import abc
import logging
import threading
import time
from typing import Dict, List, Any, Optional, Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SignalListener(abc.ABC):
    """Base abstract class for all signal listeners."""
    
    def __init__(self, name: str):
        self.name = name
        self.is_running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[Dict[str, Any]], None]] = []
        logger.info(f"Initialized {self.__class__.__name__}: {name}")
    
    def register_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback function to be called when a signal is received.
        
        Args:
            callback: Function that takes a signal data dictionary as argument
        """
        self._callbacks.append(callback)
        logger.debug(f"Registered callback for {self.name}: {callback.__name__}")
    
    def _notify_callbacks(self, signal_data: Dict[str, Any]) -> None:
        """Notify all registered callbacks with the signal data."""
        for callback in self._callbacks:
            try:
                callback(signal_data)
            except Exception as e:
                logger.error(f"Error in callback {callback.__name__} for {self.name}: {e}")
    
    def start(self) -> None:
        """Start listening for signals in a separate thread."""
        if self.is_running:
            logger.warning(f"{self.name} is already running")
            return
        
        self.is_running = True
        self._thread = threading.Thread(target=self._listen_loop, daemon=True)
        self._thread.start()
        logger.info(f"Started {self.name}")
    
    def stop(self) -> None:
        """Stop listening for signals."""
        if not self.is_running:
            logger.warning(f"{self.name} is not running")
            return
        
        self.is_running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning(f"Failed to stop {self.name} gracefully")
            else:
                logger.info(f"Stopped {self.name}")
        
        self._thread = None
    
    @abc.abstractmethod
    def _listen_loop(self) -> None:
        """Main listening loop to be implemented by subclasses."""
        pass


class SignalListenerManager:
    """Manager class for handling multiple signal listeners."""
    
    def __init__(self):
        self.listeners: Dict[str, SignalListener] = {}
        logger.info("Initialized SignalListenerManager")
    
    def add_listener(self, listener: SignalListener) -> None:
        """Add a listener to the manager.
        
        Args:
            listener: SignalListener instance to add
        """
        self.listeners[listener.name] = listener
        logger.info(f"Added listener: {listener.name}")
    
    def remove_listener(self, name: str) -> None:
        """Remove a listener from the manager.
        
        Args:
            name: Name of the listener to remove
        """
        if name in self.listeners:
            listener = self.listeners[name]
            if listener.is_running:
                listener.stop()
            del self.listeners[name]
            logger.info(f"Removed listener: {name}")
        else:
            logger.warning(f"Listener not found: {name}")
    
    def start_all(self) -> None:
        """Start all registered listeners."""
        for name, listener in self.listeners.items():
            listener.start()
        logger.info(f"Started all listeners: {', '.join(self.listeners.keys())}")
    
    def stop_all(self) -> None:
        """Stop all registered listeners."""
        for name, listener in self.listeners.items():
            listener.stop()
        logger.info(f"Stopped all listeners: {', '.join(self.listeners.keys())}")
    
    def register_global_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for all listeners.
        
        Args:
            callback: Function that takes a signal data dictionary as argument
        """
        for listener in self.listeners.values():
            listener.register_callback(callback)
        logger.info(f"Registered global callback: {callback.__name__}")
