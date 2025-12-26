"""Base destination interface with retry logic."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_attempts: int = 3
    min_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    multiplier: float = 2.0


class BaseDestination(ABC):
    """
    Base class for all destinations.
    
    Destinations are responsible for:
    1. Sending transformed data
    2. Health checks
    3. Retry logic with exponential backoff
    4. Metrics reporting
    """
    
    def __init__(self, config: Dict[str, Any], retry_config: Optional[RetryConfig] = None):
        """
        Initialize destination with configuration.
        
        Args:
            config: Destination-specific configuration
            retry_config: Optional retry configuration
        """
        self.config = config
        self.retry_config = retry_config or RetryConfig()
    
    @abstractmethod
    def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to destination.
        
        Args:
            records: List of records to send
            metadata: Optional metadata (execution_id, batch_id, etc.)
        
        Raises:
            DestinationError: If send fails after retries
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if destination is healthy and accessible.
        
        Returns:
            True if healthy, False otherwise
        """
        pass
    
    def send_with_retry(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records with automatic retry and exponential backoff.
        
        Args:
            records: Records to send
            metadata: Optional metadata
        """
        retry_decorator = retry(
            stop=stop_after_attempt(self.retry_config.max_attempts),
            wait=wait_exponential(
                multiplier=self.retry_config.multiplier,
                min=self.retry_config.min_wait_seconds,
                max=self.retry_config.max_wait_seconds,
            ),
            retry=retry_if_exception_type(DestinationError),
            reraise=True,
        )
        
        retry_send = retry_decorator(self.send)
        retry_send(records, metadata)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


class DestinationError(Exception):
    """Raised when a destination operation fails."""
    
    def __init__(self, destination_type: str, message: str, original_error: Exception = None):
        self.destination_type = destination_type
        self.original_error = original_error
        super().__init__(f"Destination '{destination_type}' error: {message}")
