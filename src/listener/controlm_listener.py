"""
Control-M Signal Listener

This module provides functionality to listen for signals from Control-M.
Since the specific API specs for Control-M are not available, this is a stubbed implementation.
"""

import logging
import time
import json
import requests
from typing import Dict, Any, Optional, List

from .signal_listener import SignalListener

logger = logging.getLogger(__name__)


class ControlMListener(SignalListener):
    """Listener for Control-M signals (stubbed implementation)."""
    
    def __init__(self, 
                 name: str = "controlm_listener",
                 endpoint: str = "http://localhost:8080/api",
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 api_key: Optional[str] = None,
                 poll_interval: float = 60.0,
                 job_filters: Optional[Dict[str, Any]] = None):
        """Initialize Control-M listener.
        
        Args:
            name: Unique name for this listener
            endpoint: Control-M API endpoint URL
            username: Control-M API username
            password: Control-M API password
            api_key: Control-M API key (alternative to username/password)
            poll_interval: How often to poll for new signals (seconds)
            job_filters: Filters to apply when querying jobs
        """
        super().__init__(name)
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.api_key = api_key
        self.poll_interval = poll_interval
        self.job_filters = job_filters or {}
        self.auth_token = None
        self.token_expiry = 0
        
        logger.info(f"Initialized ControlMListener with poll interval: {poll_interval}s")
    
    def _get_auth_token(self) -> Optional[str]:
        """Get authentication token for Control-M API.
        
        Returns:
            Authentication token or None if authentication failed
        """
        # Check if current token is still valid
        current_time = time.time()
        if self.auth_token and current_time < self.token_expiry - 60:
            return self.auth_token
            
        try:
            # STUB: In a real implementation, this would make an actual API call
            # to the Control-M authentication endpoint
            
            # Example implementation (not functional)
            auth_url = f"{self.endpoint}/session/login"
            
            if self.api_key:
                headers = {"X-API-KEY": self.api_key}
                auth_data = {}
            else:
                headers = {"Content-Type": "application/json"}
                auth_data = {
                    "username": self.username,
                    "password": self.password
                }
            
            # This is a stub and would be replaced with actual API call
            # response = requests.post(auth_url, headers=headers, json=auth_data)
            # response.raise_for_status()
            # token_data = response.json()
            # self.auth_token = token_data.get("token")
            # expires_in = token_data.get("expires_in", 3600)
            
            # For stub purposes, just create a dummy token
            self.auth_token = "dummy_token_for_controlm"
            expires_in = 3600
            self.token_expiry = current_time + expires_in
            
            logger.debug("Successfully obtained Control-M auth token (stub)")
            return self.auth_token
            
        except Exception as e:
            logger.error(f"Failed to get Control-M auth token: {e}")
            return None
    
    def _query_jobs(self) -> List[Dict[str, Any]]:
        """Query jobs from Control-M.
        
        Returns:
            List of job data dictionaries
        """
        # STUB: In a real implementation, this would query the Control-M API
        # for job status information
        
        token = self._get_auth_token()
        if not token:
            return []
            
        try:
            # Example implementation (not functional)
            jobs_url = f"{self.endpoint}/jobs"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Apply filters
            params = self.job_filters.copy() if self.job_filters else {}
            
            # This is a stub and would be replaced with actual API call
            # response = requests.get(jobs_url, headers=headers, params=params)
            # response.raise_for_status()
            # return response.json()
            
            # For stub purposes, return dummy data
            return [
                {
                    "job_id": "job123",
                    "name": "Daily Batch Process",
                    "folder": "Finance",
                    "status": "Completed",
                    "start_time": "2025-05-14T10:00:00Z",
                    "end_time": "2025-05-14T10:15:00Z",
                    "output": "Process completed successfully"
                },
                {
                    "job_id": "job456",
                    "name": "Weekly Report",
                    "folder": "Reporting",
                    "status": "Running",
                    "start_time": "2025-05-14T11:00:00Z",
                    "end_time": None,
                    "output": None
                }
            ]
            
        except Exception as e:
            logger.error(f"Failed to query Control-M jobs: {e}")
            return []
    
    def _query_alerts(self) -> List[Dict[str, Any]]:
        """Query alerts from Control-M.
        
        Returns:
            List of alert data dictionaries
        """
        # STUB: In a real implementation, this would query the Control-M API
        # for alerts
        
        token = self._get_auth_token()
        if not token:
            return []
            
        try:
            # Example implementation (not functional)
            alerts_url = f"{self.endpoint}/alerts"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # This is a stub and would be replaced with actual API call
            # response = requests.get(alerts_url, headers=headers)
            # response.raise_for_status()
            # return response.json()
            
            # For stub purposes, return dummy data
            return [
                {
                    "alert_id": "alert789",
                    "severity": "Warning",
                    "message": "Job taking longer than expected",
                    "job_id": "job456",
                    "timestamp": "2025-05-14T11:30:00Z"
                }
            ]
            
        except Exception as e:
            logger.error(f"Failed to query Control-M alerts: {e}")
            return []
    
    def _fetch_signals(self) -> Dict[str, Any]:
        """Fetch signals from Control-M.
        
        Returns:
            Dictionary containing signal data
        """
        jobs = self._query_jobs()
        alerts = self._query_alerts()
        
        return {
            "source": "controlm",
            "timestamp": time.time(),
            "jobs": jobs,
            "alerts": alerts
        }
    
    def _listen_loop(self) -> None:
        """Main listening loop for Control-M signals."""
        logger.info(f"{self.name} listening loop started")
        
        while self.is_running:
            try:
                signals = self._fetch_signals()
                if signals and (signals.get("jobs") or signals.get("alerts")):
                    self._notify_callbacks(signals)
                
                # Sleep for the poll interval
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in Control-M listening loop: {e}")
                # Sleep briefly before retrying
                time.sleep(5)
