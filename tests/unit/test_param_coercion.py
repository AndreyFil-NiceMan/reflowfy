"""Coercion of string parameter values into their declared types.

Strings arrive from every entry point — the CLI's `--param key=value` and its
prompts, and JSON bodies posted to /run — so a list parameter must end up a
list whatever shape the string took.
"""

from reflowfy.core.abstract_pipeline import PipelineParameter


def list_param() -> PipelineParameter:
    return PipelineParameter(name="ids", param_type=list, required=True)


def test_json_list_is_parsed():
    assert list_param().coerce("[1, 2, 3]") == [1, 2, 3]


def test_comma_separated_becomes_a_list():
    assert list_param().coerce("7,8") == ["7", "8"]


def test_single_value_becomes_a_one_element_list():
    # Regression: a bare scalar is valid JSON, so "3" used to parse as the int 3
    # and hand the pipeline a non-iterable for a list parameter.
    assert list_param().coerce("3") == ["3"]


def test_single_non_numeric_value_becomes_a_one_element_list():
    assert list_param().coerce("abc") == ["abc"]


def test_json_scalar_true_is_not_mistaken_for_a_list():
    assert list_param().coerce("true") == ["true"]


def test_whitespace_is_trimmed_and_blanks_dropped():
    assert list_param().coerce(" 7 , , 8 ") == ["7", "8"]


def test_already_a_list_passes_through():
    assert list_param().coerce([1, 2]) == [1, 2]
