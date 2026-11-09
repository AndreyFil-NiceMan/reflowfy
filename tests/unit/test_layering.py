"""Imports point down the stack: core at the bottom, then connectors, then the services.

A module that imports a more volatile one has to change whenever it does; these rules
make that visible instead of leaving it to whoever adds the import. Imports under
``if TYPE_CHECKING:`` do not count (they never run).
"""

import ast
import pathlib
from typing import Dict, Iterator, Set, Tuple

import pytest

PACKAGE = pathlib.Path(__file__).resolve().parents[2] / "reflowfy"

SERVICES = {"execution", "worker", "reflow_manager", "api", "cli"}
CONNECTORS = {"sources", "destinations", "transformations", "factories"}

# package -> packages it must never import from at runtime
FORBIDDEN: Dict[str, Set[str]] = {
    "core": CONNECTORS | SERVICES,
    "sources": SERVICES | {"destinations", "transformations"},
    "destinations": SERVICES | {"sources", "transformations", "factories"},
    "transformations": SERVICES | {"sources", "destinations", "factories"},
    "factories": SERVICES,
}

# Known exceptions, each with the fix that removes it (docs/untangle-plan.md).
ALLOWED: Set[Tuple[str, str]] = {
    # F2: define_source may return a list, which the ID-based plan wraps in a
    # StaticSource; it goes once planning returns job envelopes instead.
    ("core/id_based_pipeline.py", "sources"),
}


def _runtime_imports(tree: ast.Module) -> Iterator[str]:
    type_checking_blocks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and getattr(node.test, "id", getattr(node.test, "attr", None)) == "TYPE_CHECKING"
    ]
    skipped = {id(n) for block in type_checking_blocks for n in ast.walk(block)}
    for node in ast.walk(tree):
        if id(node) in skipped:
            continue
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            yield node.module
        elif isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)


def _violations() -> Iterator[str]:
    for package, forbidden in FORBIDDEN.items():
        for path in sorted((PACKAGE / package).rglob("*.py")):
            relative = str(path.relative_to(PACKAGE))
            for module in _runtime_imports(ast.parse(path.read_text())):
                parts = module.split(".")
                if parts[0] != "reflowfy" or len(parts) < 2 or parts[1] not in forbidden:
                    continue
                if (relative, parts[1]) not in ALLOWED:
                    yield f"{relative} imports {module}"


def test_imports_point_down_the_stack() -> None:
    assert list(_violations()) == []


@pytest.mark.parametrize("relative, package", sorted(ALLOWED))
def test_every_allowed_exception_is_still_needed(relative: str, package: str) -> None:
    """An exception whose import is gone must be removed from ALLOWED too."""
    tree = ast.parse((PACKAGE / relative).read_text())
    assert any(m.split(".")[1:2] == [package] for m in _runtime_imports(tree))
