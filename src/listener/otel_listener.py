"""
OpenTelemetry Signal Listener

This module provides functionality to listen for signals using OpenTelemetry.
It can be configured to work with various backends including DataDog.
"""

import logging
import time
import json
from typing import Dict, Any, Optional, List

from .signal_listener import SignalListener

logger = logging.getLogger(__name__)

# Import OpenTelemetry packages conditionally to avoid hard dependency
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    import requests
    
    OTEL_AVAILABLE = True
except ImportError:
    logger.warning("OpenTelemetry packages not found. OTelListener will be limited.")
    OTEL_AVAILABLE = False


class OTelListener(SignalListener):
    """Listener for signals using OpenTelemetry."""
    
    def __init__(self, 
                 name: str = "otel_listener",
                 api_key: Optional[str] = None,
                 app_key: Optional[str] = None,
                 site: str = "datadoghq.com",
                 poll_interval: float = 60.0,
                 metrics: List[str] = None,
                 monitors: List[int] = None,
                 backend: str = "datadog",
                 endpoint: Optional[str] = None):
        """Initialize OpenTelemetry listener.
        
        Args:
            name: Unique name for this listener
            api_key: Backend API key (e.g., DataDog API key)
            app_key: Backend Application key (e.g., DataDog Application key)
            site: Backend site (e.g., 'datadoghq.com', 'datadoghq.eu')
            poll_interval: How often to poll for new signals (seconds)
            metrics: List of metric names to monitor
            monitors: List of monitor IDs to check
            backend: Backend service name (e.g., 'datadog', 'newrelic')
            endpoint: Custom endpoint URL (overrides site if provided)
        """
        super().__init__(name)
        self.api_key = api_key
        self.app_key = app_key
        self.site = site
        self.poll_interval = poll_interval
        self.metrics = metrics or []
        self.monitors = monitors or []
        self.backend = backend
        self.endpoint = endpoint
        self.use_otel = OTEL_AVAILABLE
        
        # Set the API base URL based on the backend
        if self.endpoint:
            self.api_base_url = self.endpoint
        elif self.backend == "datadog":
            self.api_base_url = f"https://api.{self.site}/api/v1"
        else:
            self.api_base_url = f"https://api.{self.site}"
        
        if self.use_otel:
            self._setup_otel()
            
        logger.info(f"Initialized OTelListener with backend: {backend}, poll interval: {poll_interval}s")
        if self.metrics:
            logger.info(f"Monitoring metrics: {', '.join(self.metrics)}")
        if self.monitors:
            logger.info(f"Checking monitors: {', '.join(map(str, self.monitors))}")
    
    def _setup_otel(self) -> None:
        """Set up OpenTelemetry for backend integration."""
        if not OTEL_AVAILABLE:
            logger.error("Cannot set up OpenTelemetry: required packages not installed")
            return
            
        try:
            # Set up trace provider
            resource = Resource(attributes={"service.name": self.name})
            trace_provider = TracerProvider(resource=resource)
            
            # Configure exporter based on backend
            if self.backend == "datadog":
                span_exporter = OTLPSpanExporter(
                    endpoint=f"https://trace.{self.site}/api/v2/traces",
                    headers={"DD-API-KEY": self.api_key} if self.api_key else {}
                )
            else:
                # Generic OTLP endpoint
                span_exporter = OTLPSpanExporter(
                    endpoint=self.endpoint or "http://localhost:4318/v1/traces",
                    headers={"api-key": self.api_key} if self.api_key else {}
                )
            
            trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
            trace.set_tracer_provider(trace_provider)
            
            # Set up metrics
            if self.backend == "datadog":
                metric_endpoint = f"https://api.{self.site}/api/v2/series"
                headers = {"DD-API-KEY": self.api_key} if self.api_key else {}
            else:
                metric_endpoint = self.endpoint or "http://localhost:4318/v1/metrics"
                headers = {"api-key": self.api_key} if self.api_key else {}
                
            metric_exporter = OTLPMetricExporter(
                endpoint=metric_endpoint,
                headers=headers
            )
            
            reader = PeriodicExportingMetricReader(metric_exporter)
            meter_provider = MeterProvider(metric_readers=[reader], resource=resource)
            
            logger.info(f"OpenTelemetry setup completed for {self.backend}")
            
        except Exception as e:
            logger.error(f"Failed to set up OpenTelemetry: {e}")
    
    def _make_api_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Make a request to the backend API.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            
        Returns:
            API response as dictionary or None if request failed
        """
        if not self.api_key:
            logger.error(f"{self.backend} API key is required")
            return None
            
        url = f"{self.api_base_url}/{endpoint}"
        
        # Set up headers based on backend
        if self.backend == "datadog":
            headers = {
                "DD-API-KEY": self.api_key,
                "DD-APPLICATION-KEY": self.app_key,
                "Content-Type": "application/json"
            }
        else:
            headers = {
                "api-key": self.api_key,
                "Content-Type": "application/json"
            }
            if self.app_key:
                headers["app-key"] = self.app_key
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"{self.backend} API request failed: {e}")
            return None
    
    def _check_monitors(self) -> List[Dict[str, Any]]:
        """Check the status of specified monitors.
        
        Returns:
            List of monitor status data
        """
        results = []
        
        if not self.monitors:
            return results
            
        try:
            # Get all monitors if specific IDs are provided
            if self.monitors:
                if self.backend == "datadog":
                    monitor_data = self._make_api_request("monitor", {"monitor_ids": ",".join(map(str, self.monitors))})
                else:
                    monitor_data = self._make_api_request("monitors", {"ids": ",".join(map(str, self.monitors))})
            else:
                if self.backend == "datadog":
                    monitor_data = self._make_api_request("monitor")
                else:
                    monitor_data = self._make_api_request("monitors")
                
            if not monitor_data:
                return results
                
            for monitor in monitor_data:
                if not self.monitors or monitor.get("id") in self.monitors:
                    # Standardize monitor data format across backends
                    monitor_info = {
                        "id": monitor.get("id"),
                        "name": monitor.get("name"),
                        "status": monitor.get("overall_state") or monitor.get("status"),
                        "type": monitor.get("type"),
                        "message": monitor.get("message"),
                        "timestamp": time.time()
                    }
                    results.append(monitor_info)
                    
            return results
            
        except Exception as e:
            logger.error(f"Failed to check monitors: {e}")
            return results
    
    def _query_metrics(self) -> List[Dict[str, Any]]:
        """Query specified metrics.
        
        Returns:
            List of metric data
        """
        results = []
        
        if not self.metrics:
            return results
            
        try:
            end_time = int(time.time())
            start_time = end_time - int(self.poll_interval)
            
            for metric in self.metrics:
                if self.backend == "datadog":
                    query_params = {
                        "query": f"avg:{metric}{{*}}",
                        "from": start_time,
                        "to": end_time
                    }
                    endpoint = "query"
                else:
                    query_params = {
                        "name": metric,
                        "start": start_time,
                        "end": end_time
                    }
                    endpoint = "metrics/query"
                
                metric_data = self._make_api_request(endpoint, query_params)
                
                if not metric_data:
                    continue
                    
                # Extract and standardize metric data
                if self.backend == "datadog" and "series" in metric_data:
                    for series in metric_data["series"]:
                        results.append({
                            "metric": metric,
                            "scope": series.get("scope"),
                            "expression": series.get("expression"),
                            "points": series.get("pointlist"),
                            "timestamp": time.time()
                        })
                elif "results" in metric_data:
                    # Generic format for other backends
                    for result in metric_data["results"]:
                        results.append({
                            "metric": metric,
                            "scope": result.get("scope"),
                            "points": result.get("values"),
                            "timestamp": time.time()
                        })
                        
            return results
            
        except Exception as e:
            logger.error(f"Failed to query metrics: {e}")
            return results
    
    def _fetch_signals(self) -> Dict[str, Any]:
        """Fetch signals from the configured backend.
        
        Returns:
            Dictionary containing signal data
        """
        monitor_data = self._check_monitors()
        metric_data = self._query_metrics()
        
        return {
            "source": self.backend,
            "timestamp": time.time(),
            "monitors": monitor_data,
            "metrics": metric_data
        }
    
    def _listen_loop(self) -> None:
        """Main listening loop for signals."""
        logger.info(f"{self.name} listening loop started")
        
        while self.is_running:
            try:
                signals = self._fetch_signals()
                if signals and (signals.get("monitors") or signals.get("metrics")):
                    self._notify_callbacks(signals)
                
                # Sleep for the poll interval
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in {self.name} listening loop: {e}")
                # Sleep briefly before retrying
                time.sleep(5)
