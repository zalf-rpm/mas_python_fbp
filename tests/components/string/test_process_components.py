from __future__ import annotations

from tests.component_harness import done_message, ip_message, run_process_component, text_outputs
from zalfmas_fbp.components.string.split_string2 import SplitString
from zalfmas_fbp.components.string.split_string2 import meta as split_string_meta
from zalfmas_fbp.components.string.to_string import ToString
from zalfmas_fbp.components.string.to_string import meta as to_string_meta


def test_split_string2_uses_default_config_and_writes_split_values() -> None:
    component = SplitString(split_string_meta)

    writer = run_process_component(
        component,
        in_messages=[
            ip_message("alpha,beta,gamma\n"),
            done_message(),
        ],
    )

    assert component.config["split_at"].t == ","
    assert text_outputs(writer) == ["alpha", "beta", "gamma"]


def test_to_string_can_start_with_default_config() -> None:
    component = ToString(to_string_meta)

    writer = run_process_component(
        component,
        in_messages=[
            ip_message("alpha"),
            done_message(),
        ],
    )

    assert len(writer.values) == 1
