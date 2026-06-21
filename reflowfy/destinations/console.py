"""Console destination for testing and debugging."""

import json
from typing import Any, Dict, List, Optional
from reflowfy.destinations.base import BaseDestination, RetryConfig


class ConsoleDestination(BaseDestination):
    """
    Console/stdout destination for testing.

    Prints records to console - perfect for local testing without external systems.
    """

    def __init__(
        self,
        pretty_print: bool = True,
        max_records_display: int = 10,
        retry_config: Optional[RetryConfig] = None,
    ):
        """
        Initialize console destination.

        Args:
            pretty_print: Whether to pretty-print JSON
            max_records_display: Maximum records to display (rest are summarized)
            retry_config: Optional retry configuration
        """
        config = {
            "pretty_print": pretty_print,
            "max_records_display": max_records_display,
        }
        super().__init__(config, retry_config)

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to console (print to stdout).

        Args:
            records: List of records to print
            metadata: Optional metadata
        """
        pretty_print = self.config["pretty_print"]
        max_display = self.config["max_records_display"]

        print(f"\n{'=' * 80}")
        print(f"📤 CONSOLE DESTINATION - Sending {len(records)} records")
        print(f"{'=' * 80}\n")

        if metadata:
            print("📋 Metadata:")
            if pretty_print:
                print(json.dumps(metadata, indent=2))
            else:
                print(metadata)
            print()

        # Display records
        records_to_show = records[:max_display]

        print(f"📦 Records (showing {len(records_to_show)} of {len(records)}):\n")

        for i, record in enumerate(records_to_show, 1):
            if pretty_print:
                print(f"Record {i}:")
                print(json.dumps(record, indent=2))
            else:
                print(f"Record {i}: {record}")
            print()

        if len(records) > max_display:
            print(f"... and {len(records) - max_display} more records")

        print(f"{'=' * 80}\n")
        print(f"✅ Successfully sent {len(records)} records to console\n")

    async def health_check(self) -> bool:
        """Console is always healthy."""
        return True


def console_destination(
    pretty_print: bool = True,
    max_records_display: int = 10,
    retry_config: Optional[RetryConfig] = None,
) -> ConsoleDestination:
    """
    Factory function for console destination.

    Example:
        >>> destination = console_destination(
        ...     pretty_print=True,
        ...     max_records_display=5
        ... )
    """
    return ConsoleDestination(
        pretty_print=pretty_print,
        max_records_display=max_records_display,
        retry_config=retry_config,
    )
