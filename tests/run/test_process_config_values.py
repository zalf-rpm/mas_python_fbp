from __future__ import annotations

import pytest
from mas.schema.common import common_capnp

from zalfmas_fbp.run.process import Process


def test_config_value_round_trips_nested_python_values() -> None:
    original = {
        "name": "alpha",
        "count": 3,
        "enabled": True,
        "weights": [1.0, 2.5],
        "nested": {"labels": ["a", "b"], "mixed": [1, True, {"leaf": "value"}]},
    }

    capnp_value = Process._config_value_from_python(original)

    assert Process._python_value_from_capnp_value(capnp_value.as_reader()) == original


def test_config_value_rejects_non_string_dict_keys() -> None:
    with pytest.raises(TypeError, match="Config dict keys must be strings"):
        Process._config_value_from_python({1: "value"})


def test_python_value_from_capnp_rejects_malformed_pair_entries() -> None:
    malformed = common_capnp.Value.new_message(
        lpair=[
            common_capnp.Pair.new_message(fst="missing-snd"),
        ],
    )

    with pytest.raises(TypeError, match="both 'fst' and 'snd'"):
        Process._python_value_from_capnp_value(malformed.as_reader())
