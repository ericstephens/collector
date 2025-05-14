"""
Microsoft Teams Signal Listener

This module provides functionality to listen for signals from Microsoft Teams.
"""

import logging
import time
import json
import requests
from typing import Dict, Any, Optional, List

from .signal_listener import SignalListener

logger = logging.getLogger(__name__)

class TeamsListener(SignalListener):
    """Listener for Microsoft Teams signals."""
    
    def __init__(self, 
                 name: str = "teams_listener",
                 tenant_id: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 poll_interval: float = 30.0,
                 channels: Optional[List[Dict[str, str]]] = None,
                 group_chats: Optional[List[Dict[str, str]]] = None):
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
        self.channels = channels or []
        self.group_chats = group_chats or []
        self.access_token = None
        self.token_expiry = 0
        self.last_check_time = {}
        
        # Initialize last check time for each channel and group chat
        for channel in self.channels:
            channel_key = f"channel:{channel['team_id']}:{channel['channel_id']}"
            self.last_check_time[channel_key] = time.time()
            
        for chat in self.group_chats:
            chat_key = f"chat:{chat['chat_id']}"
            self.last_check_time[chat_key] = time.time()
            
        logger.info(f"Initialized TeamsListener with poll interval: {poll_interval}s")
        if self.channels:
            logger.info(f"Monitoring {len(self.channels)} Teams channels")
        if self.group_chats:
            logger.info(f"Monitoring {len(self.group_chats)} Teams group chats")
    
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
    
    def _fetch_channel_messages(self, team_id: str, channel_id: str, channel_name: str) -> Optional[Dict[str, Any]]:
        """Fetch messages from a specific Teams channel.
        
        Args:
            team_id: Team ID
            channel_id: Channel ID
            channel_name: Channel name for logging and identification
            
        Returns:
            Dictionary containing message data or None if failed
        """
        token = self._get_auth_token()
        if not token:
            return None
            
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get the last check time for this channel
            channel_key = f"channel:{team_id}:{channel_id}"
            last_time = self.last_check_time.get(channel_key, time.time() - self.poll_interval)
            
            # Format the datetime for the filter
            # Microsoft Graph API uses ISO 8601 format
            from datetime import datetime, timezone
            iso_time = datetime.fromtimestamp(last_time, tz=timezone.utc).isoformat()
            
            # Use filter to get only messages after the last check time
            url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages?$filter=lastModifiedDateTime gt {iso_time}"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Update the last check time for this channel
            self.last_check_time[channel_key] = time.time()
            
            if 'value' in data and data['value']:
                logger.info(f"Found {len(data['value'])} new messages in channel '{channel_name}'")
                return {
                    'source': 'microsoft_teams',
                    'type': 'channel_message',
                    'team_id': team_id,
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'timestamp': time.time(),
                    'messages': data['value']
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch Teams channel messages from '{channel_name}': {e}")
            return None
    
    def _fetch_chat_messages(self, chat_id: str, chat_name: str) -> Optional[Dict[str, Any]]:
        """Fetch messages from a specific Teams group chat.
        
        Args:
            chat_id: Chat ID
            chat_name: Chat name for logging and identification
            
        Returns:
            Dictionary containing message data or None if failed
        """
        token = self._get_auth_token()
        if not token:
            return None
            
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get the last check time for this chat
            chat_key = f"chat:{chat_id}"
            last_time = self.last_check_time.get(chat_key, time.time() - self.poll_interval)
            
            # Format the datetime for the filter
            from datetime import datetime, timezone
            iso_time = datetime.fromtimestamp(last_time, tz=timezone.utc).isoformat()
            
            # Use filter to get only messages after the last check time
            url = f"https://graph.microsoft.com/v1.0/chats/{chat_id}/messages?$filter=lastModifiedDateTime gt {iso_time}"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Update the last check time for this chat
            self.last_check_time[chat_key] = time.time()
            
            if 'value' in data and data['value']:
                logger.info(f"Found {len(data['value'])} new messages in group chat '{chat_name}'")
                return {
                    'source': 'microsoft_teams',
                    'type': 'group_chat_message',
                    'chat_id': chat_id,
                    'chat_name': chat_name,
                    'timestamp': time.time(),
                    'messages': data['value']
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch Teams group chat messages from '{chat_name}': {e}")
            return None
    
    def _fetch_signals(self) -> List[Dict[str, Any]]:
        """Fetch signals from Microsoft Teams channels and group chats.
        
        Returns:
            List of dictionaries containing signal data
        """
        signals = []
        
        # Check for new messages in channels
        for channel in self.channels:
            team_id = channel.get('team_id')
            channel_id = channel.get('channel_id')
            channel_name = channel.get('name', f"{team_id}:{channel_id}")
            
            if team_id and channel_id:
                channel_data = self._fetch_channel_messages(team_id, channel_id, channel_name)
                if channel_data:
                    signals.append(channel_data)
            else:
                logger.warning(f"Skipping channel with missing team_id or channel_id: {channel}")
        
        # Check for new messages in group chats
        for chat in self.group_chats:
            chat_id = chat.get('chat_id')
            chat_name = chat.get('name', chat_id)
            
            if chat_id:
                chat_data = self._fetch_chat_messages(chat_id, chat_name)
                if chat_data:
                    signals.append(chat_data)
            else:
                logger.warning(f"Skipping group chat with missing chat_id: {chat}")
        
        return signals
    
    def _listen_loop(self) -> None:
        """Main listening loop for Teams signals."""
        logger.info(f"{self.name} listening loop started")
        
        while self.is_running:
            try:
                signals_list = self._fetch_signals()
                
                # Notify callbacks for each signal
                for signal in signals_list:
                    self._notify_callbacks(signal)
                
                # Sleep for the poll interval
                time.sleep(self.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in Teams listening loop: {e}")
                # Sleep briefly before retrying
                time.sleep(5)
