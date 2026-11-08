"""The one {field} template engine every destination uses (core/record_templates.py)."""

from urllib.parse import quote

import pytest

from reflowfy.core.record_templates import (
    TemplateError,
    fill,
    fill_value,
    has_placeholders,
    lookup,
)

KW = {"owner": "dest", "what": "url", "index": 1, "total": 3}


class TestLookup:
    def test_top_level_field(self):
        assert lookup({"a": 1}, "a") == 1

    def test_dotted_path_walks_nested_dicts(self):
        assert lookup({"data": {"region": {"code": "eu"}}}, "data.region.code") == "eu"

    def test_a_key_containing_a_dot_wins(self):
        assert lookup({"a.b": "literal", "a": {"b": "nested"}}, "a.b") == "literal"

    @pytest.mark.parametrize(
        "record, path",
        [({"a": 1}, "b"), ({"a": 1}, "a.b"), ({"a": {"b": 1}}, "a.c"), ("raw", "a"), (None, "a")],
    )
    def test_missing(self, record, path):
        with pytest.raises(TemplateError):
            fill("{" + path + "}", record, **KW)


class TestFill:
    def test_fills_every_placeholder_as_text(self):
        assert fill("/u/{id}/{data.kind}", {"id": 7, "data": {"kind": "x"}}, **KW) == "/u/7/x"

    def test_text_without_placeholders_is_returned_unchanged(self):
        assert fill("plain", {}, **KW) == "plain"

    def test_non_identifier_braces_are_kept(self):
        assert fill('{"a": 1} {id}', {"id": 2}, **KW) == '{"a": 1} 2'

    def test_encode_applies_to_values_only(self):
        out = fill("/u/{id}?x", {"id": "a/b c"}, encode=lambda t: quote(t, safe=""), **KW)
        assert out == "/u/a%2Fb%20c?x"

    def test_missing_field_names_template_and_position_never_values(self):
        with pytest.raises(TemplateError) as excinfo:
            fill("/u/{id}", {"secret": "s3cret"}, **KW)
        assert str(excinfo.value) == (
            "dest: url placeholder {id} has no matching field on record 2 of 3"
        )
        assert isinstance(excinfo.value, ValueError)  # destinations do not retry it


class TestFillValue:
    def test_a_lone_placeholder_keeps_the_type(self):
        assert fill_value("{ids}", {"ids": [1, 2]}, **KW) == [1, 2]
        assert fill_value("{data.n}", {"data": {"n": 5}}, **KW) == 5

    def test_mixed_text_is_formatted(self):
        assert fill_value("id:{id}", {"id": 5}, **KW) == "id:5"

    def test_non_strings_pass_through(self):
        assert fill_value(5, {}, **KW) == 5
        assert fill_value("plain", {}, **KW) == "plain"

    def test_a_lone_missing_placeholder_fails(self):
        with pytest.raises(TemplateError, match=r"\{n\}"):
            fill_value("{n}", {}, **KW)


def test_has_placeholders():
    assert has_placeholders("x-{tenant}")
    assert has_placeholders("{data.region}")
    assert not has_placeholders('{"a": 1}')
    assert not has_placeholders("plain")
    assert not has_placeholders(5)
