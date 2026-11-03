"""spotpy_comp: what run() does when the parameters cannot be used."""

from __future__ import annotations

import json

from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.spotpy.spotpy_comp import METADATA, Component


def test_an_invalid_derived_expression_stops_cleanly_instead_of_sampling_without_parameters(caplog) -> None:
    """The rejected parameters used to leave an empty parameter list behind, with which the next
    loop iteration started SCE-UA anyway - and that failed deep inside spotpy with an IndexError."""

    init_params = [
        {"name": "a", "low": 0.0, "high": 1.0},
        {"name": "d", "derive_expression": "1 - d"},  # refers to itself
    ]
    result = run_process_component(
        Component(METADATA),
        inputs={
            "init_params": [ip_message(json.dumps(init_params)), done_message()],
            "obs_values": [ip_message(common_capnp.Value.new_message(lf64=[1.0, 2.0])), done_message()],
            "sim_values": [done_message()],
        },
        outputs=("sampled_params", "best"),
    )

    assert result.output("sampled_params").values == []
    assert result.output("best").values == []
    assert "not calibrating with these parameters" in caplog.text
    assert "no valid parameters to calibrate with; stopping" in caplog.text
    assert "IndexError" not in caplog.text
