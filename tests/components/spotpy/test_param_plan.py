"""ParamPlan: sampled and derived calibration parameters in CSV order."""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest

from zalfmas_fbp.components.common.expressions import ExpressionError
from zalfmas_fbp.components.spotpy.spotpy_comp import ParamPlan, spotpy_parameters, write_derived_of_best

# stick-breaking: three free values in [0, 1] give four coefficients that always sum to 1
STICK_BREAKING = [
    {"name": "u1", "low": 0.0, "high": 1.0},
    {"name": "u2", "low": 0.0, "high": 1.0},
    {"name": "u3", "low": 0.0, "high": 1.0},
    {"name": "APC", "array_index": "2_0", "derive_expression": "u1"},
    {"name": "APC", "array_index": "2_1", "derive_expression": "(1 - APC_2_0) * u2"},
    {"name": "APC", "array_index": "2_2", "derive_expression": "(1 - APC_2_0 - APC_2_1) * u3"},
    {"name": "APC", "array_index": "2_3", "derive_expression": "1 - APC_2_0 - APC_2_1 - APC_2_2"},
]


@pytest.mark.parametrize("sampled", [(0.0, 0.0, 0.0), (0.3, 0.5, 0.2), (1.0, 1.0, 1.0), (0.9, 0.1, 0.7)])
def test_stick_breaking_always_sums_to_one(sampled) -> None:
    values = dict(ParamPlan(STICK_BREAKING).values(sampled))
    coeffs = [values[f"APC_2_{i}"] for i in range(4)]
    assert sum(coeffs) == pytest.approx(1.0)
    assert all(c >= 0 for c in coeffs)


def test_values_come_out_in_csv_order_with_sampled_and_derived_interleaved() -> None:
    init = [
        {"name": "a", "low": 0.0, "high": 1.0},
        {"name": "d", "derive_expression": "a + b"},
        {"name": "b", "low": 0.0, "high": 1.0},
    ]
    plan = ParamPlan(init)
    assert [p.name for p in spotpy_parameters(init)] == plan.sampled_names == ["a", "b"]
    assert plan.values([0.25, 0.5]) == [("a", 0.25), ("d", 0.75), ("b", 0.5)]


def test_v_and_the_context_can_be_used() -> None:
    init = [
        {"name": "f", "low": 0.8, "high": 1.2},
        {"name": "TSum", "array_index": 2, "derive_expression": 'cultivar["StageTemperatureSum"][0][2] * v[0]'},
    ]
    plan = ParamPlan(init, {"cultivar": {"StageTemperatureSum": [[150, 480, 300], "°C d"]}})
    assert plan.values([1.1]) == [("f", 1.1), ("TSum_2", pytest.approx(330.0))]


def test_a_derived_parameter_cannot_use_one_from_a_row_below() -> None:
    init = [
        {"name": "a", "low": 0.0, "high": 1.0},
        {"name": "d1", "derive_expression": "d2 * 2"},
        {"name": "d2", "derive_expression": "a"},
    ]
    with pytest.raises(ExpressionError, match="d1.*unknown name"):
        ParamPlan(init)


def test_a_bad_expression_fails_when_the_plan_is_built() -> None:
    init = [{"name": "a", "low": 0.0, "high": 1.0}, {"name": "d", "derive_expression": "a.__class__"}]
    with pytest.raises(ExpressionError, match="derived parameter 'd'"):
        ParamPlan(init)


def test_duplicate_names_are_rejected() -> None:
    init = [{"name": "a", "low": 0.0, "high": 1.0}, {"name": "a", "derive_expression": "1"}]
    with pytest.raises(ExpressionError, match="unique"):
        ParamPlan(init)


def test_a_name_clashing_with_the_context_is_rejected() -> None:
    with pytest.raises(ExpressionError, match="clash"):
        ParamPlan([{"name": "cultivar", "low": 0.0, "high": 1.0}], {"cultivar": {}})


@pytest.mark.parametrize("expression", ["1 / a", "a - 1 if a > 1 else 'x'", "(0 - 8) ** 0.5"])
def test_a_derived_value_that_is_not_a_finite_number_raises(expression) -> None:
    plan = ParamPlan([{"name": "a", "low": 0.0, "high": 1.0}, {"name": "d", "derive_expression": expression}])
    with pytest.raises(ExpressionError):
        plan.values([0.0])


def test_without_derived_parameters_the_sampled_ones_pass_through() -> None:
    plan = ParamPlan([{"name": "p", "array_index": 0, "low": 0.0, "high": 1.0}])
    assert not plan.derived
    assert plan.values([0.5]) == [("p_0", 0.5)]


def test_the_best_output_lists_the_derived_parameters_of_the_best_setting() -> None:
    plan = ParamPlan([{"name": "a", "low": 0.0, "high": 1.0}, {"name": "d", "derive_expression": "1 - a"}])
    stream = io.StringIO()
    write_derived_of_best(plan, SimpleNamespace(optimization_direction="minimize", params_min=[0.25]), stream)
    assert stream.getvalue() == "Derived parameters of that setting:\nd: 0.75\n\n"
