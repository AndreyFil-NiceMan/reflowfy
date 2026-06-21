"""Lifespan migration tests: no deprecated on_event handlers should remain."""

from reflowfy.api.app import create_app
import reflowfy.reflow_manager.app as rm_app


class TestApiAppLifespan:
    def test_no_on_event_startup_handlers(self):
        app = create_app()
        assert app.router.on_startup == []
        assert app.router.on_shutdown == []


class TestReflowManagerAppLifespan:
    def test_no_on_event_handlers(self):
        assert rm_app.app.router.on_startup == []
        assert rm_app.app.router.on_shutdown == []
