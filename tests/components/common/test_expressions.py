"""expressions: the safe arithmetic used for derived calibration parameters."""

from __future__ import annotations

import pytest

from zalfmas_fbp.components.common.expressions import Expression, ExpressionError

CULTIVAR = {"StageTemperatureSum": [[150, 480, 300], "°C d"], "SpecificLeafArea": [0.002, 0.003]}


def ev(source: str, **values):
    return Expression(source, values)(values)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("1 - a - b", 0.25),
        ("(1 - a) * b", 0.125),
        ("a ** 2", 0.25),
        ("-a + +b", -0.25),
        ("a // 0.2 + a % 0.2", 2.1),
        ("max(a, b) if a < b else min(a, b)", 0.25),
        ("b < a <= 0.5", True),
        ("a < b <= 0.5", False),
        ("a < b or 7", 7),
        ("clip(a * 10, 0, 1)", 1),
        ("round(sqrt(16) + exp(0) + log(1) + log10(100) + abs(-1))", 8),
        ("sum([a, b, 1]) / len([a, b, 1])", 1.75 / 3),
        ("not a", False),
    ],
)
def test_arithmetic_and_functions(source, expected) -> None:
    assert ev(source, a=0.5, b=0.25) == pytest.approx(expected)


def test_indexing_into_nested_json_like_values() -> None:
    assert ev('cultivar["StageTemperatureSum"][0][1] * f', cultivar=CULTIVAR, f=1.5) == 720
    assert ev('cultivar["SpecificLeafArea"][-1]', cultivar=CULTIVAR) == 0.003
    assert ev("v[1] + v[0]", v=[1.0, 2.0]) == 3.0


def test_used_names_are_recorded() -> None:
    assert Expression("a * b + a", {"a", "b", "c"}).used_names == {"a", "b"}


@pytest.mark.parametrize(
    "source",
    [
        # the escape routes out of a restricted eval
        "().__class__.__base__.__subclasses__()",
        "a.__class__",
        "__import__('os').system('true')",
        "getattr(a, '__class__')",
        "eval('1')",
        "open('/etc/passwd')",
        # constructs that are simply not part of the language here
        "lambda: 1",
        "[x for x in v]",
        "{1: 2}",
        "{1, 2}",
        "f'{a}'",
        "v[0:2]",
        "max(*v)",
        "round(a, ndigits=1)",
        "(a := 1)",
        "a; b",
        "1j",
        "a << 2",
        "min",
        "unknown + 1",
        "",
        "1 +",
        "x" * 2000,
    ],
)
def test_disallowed_expressions_are_rejected_before_evaluation(source) -> None:
    with pytest.raises(ExpressionError):
        Expression(source, {"a", "b", "v"})


@pytest.mark.parametrize(
    "source",
    [
        "9 ** 9 ** 9",  # exponent above the limit
        "((10 ** 100) ** 100) ** 100",  # stays below it, but overflows as a float
        '"a" * 100000000',  # arithmetic only on numbers: no huge strings ...
        "[0] * 100000000",  # ... or lists
        "1 / 0",
        "log(-1)",
        "v[5]",
        'c["missing"]',
        "a[0]",  # indexing a number
    ],
)
def test_runaway_or_failing_evaluation_raises_expression_error(source) -> None:
    values = {"a": 0.5, "v": [1.0], "c": {}}
    with pytest.raises(ExpressionError):
        Expression(source, values)(values)


def test_the_error_names_the_offending_part() -> None:
    with pytest.raises(ExpressionError, match=r"'a.__class__' is attribute access"):
        Expression("1 + a.__class__", {"a"})
