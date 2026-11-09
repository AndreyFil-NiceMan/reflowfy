"""A background thread that runs one task every N seconds until stopped.

The DLQ scheduler, the cron scheduler and the retention sweeper each carried the
same ``start`` / ``stop`` / ``_run_loop`` body; this is that body once.
"""

import logging
import threading
from typing import ClassVar, Optional


class PeriodicTask:
    """Calls :meth:`tick` every :attr:`interval_seconds` on a daemon thread.

    A tick that raises is logged (``error_event``) and the loop goes on: a database
    restart must not end the schedule. :meth:`stop` interrupts the wait at once.
    Log lines go to the subclass's module logger, so they keep its name.
    """

    #: Name used in the start/stop/error log lines, e.g. "DLQ Scheduler".
    label: ClassVar[str] = "Periodic task"
    error_event: ClassVar[str] = "scheduler_error"

    def __init__(self) -> None:
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._log = logging.getLogger(type(self).__module__)

    @property
    def interval_seconds(self) -> float:
        raise NotImplementedError

    def tick(self) -> None:
        raise NotImplementedError

    def _log_started(self) -> None:
        self._log.info(
            "%s started (every %ss)",
            self.label,
            self.interval_seconds,
            extra={"event": "scheduler_start", "interval_s": self.interval_seconds},
        )

    def start(self) -> None:
        """Start the thread; a second call while it runs only warns."""
        if self._running:
            self._log.warning(
                "%s already running", self.label, extra={"event": "scheduler_already_running"}
            )
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._log_started()

    def stop(self) -> None:
        """Stop the thread, waiting up to 10 s for a tick in progress."""
        if not self._running:
            return
        self._log.info("Stopping %s", self.label, extra={"event": "scheduler_stopping"})
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        self._log.info("%s stopped", self.label, extra={"event": "scheduler_stopped"})

    def _run_loop(self) -> None:
        while self._running:
            try:
                self.tick()
            except Exception:
                self._log.error(
                    "%s error", self.label, exc_info=True, extra={"event": self.error_event}
                )
            self._stop_event.wait(timeout=self.interval_seconds)
