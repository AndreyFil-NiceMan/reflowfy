Type contracts, checked by tests/unit/test_type_contracts.py with `pyright --outputjson`.

pass_*.py  must type-check clean under the repo's strict [tool.pyright] settings.
fail_*.py  must produce an error. The expected rule is declared on the line that
           should error with a trailing `# expect: <pyright rule>` comment.

These files are never imported or executed, and [tool.pyright] `include` does not
cover tests/typecheck/, so the deliberate errors in fail_*.py never reach the main
`uv run pyright` run.
