"""How often E2E tests poll the stack for a state they are waiting on.

Every wait loop in the suite polls a local service (the manager's stats route, a mock
sink, Kafka), so a short interval costs nothing, while a long one adds about half an
interval to every wait: at the old 2 s, ~140 waits spent minutes asleep after the
state they were waiting for had already arrived. Override with E2E_POLL_INTERVAL.

Only loops that poll *until* something happens use this. Sleeps that hold still on
purpose (checking that something does NOT happen, e.g. a paused execution staying
paused) keep their own fixed durations.
"""

import os

POLL_INTERVAL = float(os.getenv("E2E_POLL_INTERVAL", "0.25"))
