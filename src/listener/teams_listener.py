"""
Microsoft Teams Signal Listener

This module provides functionality to listen for signals from Microsoft Teams.
"""

import logging
import time
import json
import requests
from typing import Dict, Any, Optional

from .signal_listener import SignalListener

logger = logging.getLogger(__name__)

class TeamsListener(SignalListener):
    """Listener for Microsoft Teams signals."""
    
    def __init__(self, 
                 name: str = "teams_listener",
                 tenant_id: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 poll_interval: float = 30.0):
        """Initialize Teams listener.
        
        Args:
            name: Unique name for this listener
            tenant_id: Microsoft Teams tenant ID
            client_id: Microsoft Teams client ID
            client_secret: Microsoft Teams client secret
            poll_interval: How often to poll for new signals (seconds)
        """
        super().__init__(name)
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.poll_interval = poll_interval
        self.access_token = None
        self.token_expiry = 0
        logger.info(f"Initialized TeamsListener with poll interval: {poll_interval}s")
    
    def _get_auth_token(self) -> Optional[str]:
        """Get Microsoft Graph API authentication token.
        
        Returns:
            Authentication token or None if authentication failed
        """
        if not all([self.tenant_id, self.client_id, self.client_secret]):
            logger.error("Teams authentication credentials not provided")
            return None
            
        # Check if current token is still valid
        current_time = time.time()
        if self.access_token and current_time < self.token_expiry - 60:
            return self.access_token
            
        try:
            token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            payload = {
                'client_id': self.client_id,
                'scope': 'https://graph.microsoft.com/.default',
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials'
            }
            
            response = requests.post(token_url, data=payload)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            expires_in = token_data.get('expires_in', 3600)
            self.token_expiry = current_time + expires_in
            
            logger.debug("Successfully obtained Teams auth token")
            return self.access_token
            
        except Exception as e:
            logger.error(f"Failed to get Teams auth token: {e}")
            return None
    
    def _fetch_signals(self) -> Optional[Dict[str, Any]]:
        """Fetch signals from Microsoft Teams.
        
        Returns:
            Dictionary containing signal data or None if failed
        """
        token = self._get_auth_token()
        if not token:
            return None
            
        try:
            # This is a simplified example - in a real implementation, you would
            # use the Microsoft Graph API to subscribe to specific events
            # https://docs.microsoft.com/en-us/graph/api/resources/webhooks
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Example: Get recent messages from a specific channel
            # In a real implementation, you would use change notifications or webhooks
            channel_id = "your_channel_id"  # This would be configured by the user
            team_id = "your_team_id"        # This would be configured by the user
            
            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            return {
                'source': 'microsoft_teams',
                'timestamp': time.time(),
                'data': data
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch Teams signals: {e}")
            return None
    
    def _listen_loop(self) -> None:
        """Main listening loop for Teams signals."""
        logger.info(f"{self.name} listening loop started")
        
        while self.is_running:
            try:
                signals = self._fetch_signals()
                if signals:
                    self._notify_callbacks(signals)
                
                # Sleep for the poll interval
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in Teams listening loop: {e}")
                # Sleep briefly before retrying
                time.sleep(5)
