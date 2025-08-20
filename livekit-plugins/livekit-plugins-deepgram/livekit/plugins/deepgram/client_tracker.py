"""
Simple client tracking system for Deepgram TTS connections.

This module provides basic Redis-based connection tracking using INCR/DECR operations.
"""

import logging
import os
import time
from datetime import datetime
from typing import Optional

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class ClientTracker:
    """
    Simple Redis-based connection tracker for Deepgram TTS.
    
    Tracks active connections using Redis INCR/DECR operations.
    """
    
    def __init__(self):
        """
        Initialize the client tracker with Redis connection parameters.
        
        Args:
            host: Redis host (defaults to REDIS_HOST env var or localhost)
            port: Redis port (defaults to REDIS_PORT env var or 6379)
            username: Redis username (defaults to REDIS_USERNAME env var)
            password: Redis password (defaults to REDIS_PASSWORD env var)
            redis_url: Complete Redis URL (overrides individual parameters if provided)
        """
        self.redis_key = "deepgram:active_connections"
        self.logger = logging.getLogger("livekit.plugins.deepgram.client_tracker")
        self.redis_client: Optional[redis.Redis] = None
        
        # Use provided parameters or fall back to environment variables
        self.redis_url = os.environ.get("REDIS_URL")
        self.host = os.environ.get("REDIS_HOST", "localhost")
        self.port = int(os.environ.get("REDIS_PORT", "6379"))
        self.username = os.environ.get("REDIS_USERNAME")
        self.password = os.environ.get("REDIS_PASSWORD")
        
        if not REDIS_AVAILABLE:
            self.logger.warning("redis package not available")
        elif not self.redis_url and not self.host:
            self.logger.warning("No Redis connection parameters provided")

    async def initialize(self) -> None:
        """Initialize Redis connection."""
        if not REDIS_AVAILABLE:
            return
            
        try:
            if self.redis_url:
                # Use complete Redis URL if provided
                self.redis_client = redis.from_url(self.redis_url)
                connection_info = self.redis_url
            else:
                # Use individual connection parameters
                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    decode_responses=True
                )
                connection_info = f"{self.host}:{self.port}"
                
            await self.redis_client.ping()
            self.logger.info(f"ClientTracker: Connected to Redis at {connection_info}")
        except Exception as e:
            self.logger.error(f"ClientTracker: Failed to connect to Redis: {e}")
            self.redis_client = None

    async def track_connection_created(self) -> None:
        """Track when a new connection is created using Redis INCR."""
        if not self.redis_client:
            return
            
        try:
            current_count = await self.redis_client.incr(self.redis_key)
            timestamp = datetime.now().isoformat()
            
            self.logger.info(
                f"Deepgram connection created - Active connections: {current_count} at {timestamp}"
            )
            
        except Exception as e:
            self.logger.error(f"ClientTracker: Failed to increment connection count: {e}")

    async def track_connection_closed(self) -> None:
        """Track when a connection is closed using Redis DECR."""
        if not self.redis_client:
            return
            
        try:
            current_count = await self.redis_client.decr(self.redis_key)
            # Ensure count doesn't go below zero
            if current_count < 0:
                await self.redis_client.set(self.redis_key, 0)
                current_count = 0
                
            timestamp = datetime.now().isoformat()
            
            self.logger.info(
                f"Deepgram connection closed - Active connections: {current_count} at {timestamp}"
            )
            
        except Exception as e:
            self.logger.error(f"ClientTracker: Failed to decrement connection count: {e}")

    async def get_active_connections(self) -> int:
        """Get current active connection count."""
        if not self.redis_client:
            return 0
            
        try:
            count = await self.redis_client.get(self.redis_key)
            return int(count) if count else 0
        except Exception as e:
            self.logger.error(f"ClientTracker: Failed to get connection count: {e}")
            return 0

    async def aclose(self) -> None:
        """Close the Redis connection."""
        if self.redis_client:
            try:
                await self.redis_client.close()
                self.logger.info("ClientTracker: Redis connection closed")
            except Exception as e:
                self.logger.error(f"ClientTracker: Failed to close Redis connection: {e}")
