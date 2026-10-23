"""The generated Kibana dashboard import file is internally consistent."""

import importlib.util
import json
from pathlib import Path

_SCRIPT = Path(__file__).parents[2] / "deploy/observability/kibana/build_dashboard.py"
_TEMPLATE = _SCRIPT.with_name("index-template.json")


def _build():
    spec = importlib.util.spec_from_file_location("build_dashboard", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.build()


def test_every_dashboard_panel_points_at_an_object_in_the_file():
    objects = _build()
    ids = {(o["type"], o["id"]) for o in objects}
    assert len(ids) == len(objects)
    dash = next(o for o in objects if o["type"] == "dashboard")
    panels = json.loads(dash["attributes"]["panelsJSON"])
    refs = {r["name"]: r for r in dash["references"]}
    assert len(panels) == len(refs)
    for panel in panels:
        ref = refs[f"{panel['panelIndex']}:{panel['panelRefName']}"]
        assert (ref["type"], ref["id"]) in ids


def test_lens_layers_are_consistent():
    for obj in _build():
        if obj["type"] != "lens":
            continue
        layer = obj["attributes"]["state"]["datasourceStates"]["formBased"]["layers"]["l1"]
        cols = layer["columns"]
        assert layer["columnOrder"] == list(cols)
        vis = obj["attributes"]["state"]["visualization"]
        used = []
        for lyr in vis.get("layers", [vis]):
            used += lyr.get("accessors", [])
            used += [lyr[k] for k in ("xAccessor", "splitAccessor", "metricAccessor") if k in lyr]
            used += [c["columnId"] for c in lyr.get("columns", [])]
        assert used and set(used) <= set(cols), obj["attributes"]["title"]
        for col in cols.values():
            if col["operationType"] == "terms":
                assert col["params"]["orderBy"]["columnId"] in cols
        assert obj["references"][0]["id"] == "reflowfy-logs"


def test_dashboard_fields_are_logged_by_the_code():
    """Every event.action the dashboard filters on exists in the source."""
    import re

    src = "".join(p.read_text() for p in Path(_SCRIPT.parents[3], "reflowfy").rglob("*.py"))
    logged = set(re.findall(r'"event":\s*"([a-z_]+)"', src))
    wanted = set()
    for obj in _build():
        if obj["type"] == "lens":
            q = obj["attributes"]["state"]["query"]["query"]
            for grp in re.findall(r"event\.action: (\([^)]*\)|\w+)", q):
                wanted |= {w for w in re.findall(r"[a-z_]+", grp) if w != "or"}
    assert wanted and wanted <= logged, wanted - logged


def test_index_template_is_valid_json_and_maps_strings_to_keyword():
    tpl = json.loads(_TEMPLATE.read_text())
    assert tpl["index_patterns"] == ["reflowfy-logs-*"]
    dyn = tpl["template"]["mappings"]["dynamic_templates"][0]["strings_as_keyword"]
    assert dyn["mapping"]["type"] == "keyword"
