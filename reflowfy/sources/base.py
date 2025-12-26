"""Base source interface for data fetching and job splitting."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional
from reflowfy.core.execution_context import parameter_resolver


@dataclass
class SourceJob:
    """
    Represents a single job to be processed by a worker.
    
    Attributes:
        records: Data records for this job
        metadata: Job-specific metadata (e.g., scroll_id, offset, page_num)
    """
    records: List[Any]
    metadata: Dict[str, Any]


class BaseSource(ABC):
    """
    Base class for all data sources.
    
    Sources are responsible for:
    1. Fetching data (for local mode)
    2. Splitting data into jobs (for distributed mode)
    3. Resolving runtime parameters
    4. Health checks
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize source with configuration.
        
        Args:
            config: Source-specific configuration
        """
        self.config = config
        self._resolved_config: Optional[Dict[str, Any]] = None
    
    def resolve_parameters(self, runtime_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve Jinja2 templates in configuration with runtime parameters.
        
        Args:
            runtime_params: Runtime parameters provided by user
        
        Returns:
            Resolved configuration
        """
        self._resolved_config = parameter_resolver.resolve(self.config, runtime_params)
        return self._resolved_config
    
    def get_runtime_parameters(self) -> List[str]:
        """
        Extract runtime parameter names from configuration.
        
        Returns:
            List of parameter names used in templates
        """
        return list(parameter_resolver.extract_parameters(self.config))
    
    @abstractmethod
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from source (used in local mode).
        
        Args:
            runtime_params: Runtime parameters for template resolution
            limit: Optional limit for testing (e.g., first 100 records)
        
        Returns:
            List of records
        """
        pass
    
    @abstractmethod
    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split source data into jobs for distributed processing.
        
        This is a generator that yields jobs as they're created.
        Each job contains a batch of records.
        
        Args:
            runtime_params: Runtime parameters for template resolution
            batch_size: Number of records per job
        
        Yields:
            SourceJob instances
        """
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if source is accessible and healthy.
        
        Returns:
            True if healthy, False otherwise
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


class SourceError(Exception):
    """Raised when a source operation fails."""
    
    def __init__(self, source_type: str, message: str, original_error: Exception = None):
        self.source_type = source_type
        self.original_error = original_error
        super().__init__(f"Source '{source_type}' error: {message}")
