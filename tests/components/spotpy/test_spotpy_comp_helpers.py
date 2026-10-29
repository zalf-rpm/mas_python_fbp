"""spotpy_comp helpers: narrowed error handling (plan LP5).

The component itself drives a real sampler over live ports, so these cover the pure helpers -
which is where the swallowed faults were.
"""

from __future__ import annotations

import numpy as np
import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.spotpy.spotpy_comp import (
    capnp_value_lf64_to_numpy_array_with_nan,
    check_and_possibly_add_sentinel_value,
    spotpy_parameters,
)


def attr_of(key, value):
    ip = fbp_capnp.IP.new_message()
    entries = ip.init("attributes", 1)
    entries[0].key = key
    entries[0].value = value
    return ip.attributes[0]


def test_it_builds_a_parameter_per_entry() -> None:
    params = spotpy_parameters([{"name": "pA", "low": 0.0, "high": 1.0}])
    assert [p.name for p in params] == ["pA"]


def test_an_array_index_becomes_a_name_suffix() -> None:
    """spotpy refuses two parameters with the same name."""

    params = spotpy_parameters([{"name": "pA", "low": 0.0, "high": 1.0, "array_index": 3}])
    assert [p.name for p in params] == ["pA_3"]


def test_a_key_spotpy_does_not_take_is_ignored_rather_than_fatal() -> None:
    """`Uniform(**par)` raises TypeError on an unknown key. That used to be swallowed, leaving
    the calibration with no parameters at all and no explanation - and the loader sends
    `derive_expression` for any row with a ninth column."""

    params = spotpy_parameters([{"name": "pA", "low": 0.0, "high": 1.0, "derive_expression": "x*2"}])
    assert [p.name for p in params] == ["pA"]


def test_the_optional_numeric_keys_are_passed_through() -> None:
    params = spotpy_parameters([{"name": "pA", "low": 0.0, "high": 1.0, "optguess": 0.5, "step": 0.1}])
    assert params[0].optguess == pytest.approx(0.5)
    assert params[0].step == pytest.approx(0.1)


def test_several_parameters_keep_their_own_values() -> None:
    params = spotpy_parameters(
        [
            {"name": "pA", "low": 0.0, "high": 1.0},
            {"name": "pB", "low": 2.0, "high": 3.0},
        ]
    )
    assert [p.name for p in params] == ["pA", "pB"]


def test_a_sentinel_value_is_picked_up() -> None:
    sentinels: dict = {}
    attr = attr_of("nan_sentinel", common_capnp.Value.new_message(f64=-9999.0))
    check_and_possibly_add_sentinel_value(sentinels, attr, "nan_sentinel")
    assert sentinels == {-9999.0: pytest.approx(np.nan, nan_ok=True)}


def test_an_attribute_of_another_name_is_left_alone() -> None:
    sentinels: dict = {}
    attr = attr_of("something_else", common_capnp.Value.new_message(f64=-9999.0))
    check_and_possibly_add_sentinel_value(sentinels, attr, "nan_sentinel")
    assert sentinels == {}


def test_a_sentinel_that_is_not_a_value_is_skipped() -> None:
    sentinels: dict = {}
    attr = attr_of("nan_sentinel", "not a Value")
    check_and_possibly_add_sentinel_value(sentinels, attr, "nan_sentinel")
    assert sentinels == {}


def test_a_sentinel_that_is_not_an_f64_is_skipped() -> None:
    sentinels: dict = {}
    attr = attr_of("nan_sentinel", common_capnp.Value.new_message(i64=-9999))
    check_and_possibly_add_sentinel_value(sentinels, attr, "nan_sentinel")
    assert sentinels == {}


def test_sentinels_are_replaced_by_nan() -> None:
    value = common_capnp.Value.new_message(lf64=[1.0, -9999.0, 3.0])
    array = capnp_value_lf64_to_numpy_array_with_nan(value.lf64, sentinel_values={-9999.0: np.nan})
    assert array[0] == 1.0
    assert np.isnan(array[1])
    assert array[2] == 3.0


def test_no_sentinels_leaves_the_values_alone() -> None:
    value = common_capnp.Value.new_message(lf64=[1.0, 2.0])
    assert list(capnp_value_lf64_to_numpy_array_with_nan(value.lf64)) == [1.0, 2.0]


def test_the_sentinel_default_is_not_shared_between_calls() -> None:
    """It was a mutable default argument."""

    value = common_capnp.Value.new_message(lf64=[5.0])
    first = capnp_value_lf64_to_numpy_array_with_nan(value.lf64)
    second = capnp_value_lf64_to_numpy_array_with_nan(value.lf64)
    assert list(first) == list(second) == [5.0]
