from __future__ import annotations

from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component, text_outputs
from zalfmas_fbp.components.string.split_string2 import METADATA as split_string_metadata
from zalfmas_fbp.components.string.split_string2 import SplitString
from zalfmas_fbp.components.string.to_string import METADATA as to_string_metadata
from zalfmas_fbp.components.string.to_string import ToString


def test_split_string2_uses_default_config_and_writes_split_values() -> None:
    component = SplitString(split_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("alpha,beta,gamma\n"),
                done_message(),
            ],
        },
    ).output()

    assert component.config.split_at == ","
    assert text_outputs(writer) == ["alpha", "beta", "gamma"]


def test_split_string2_reads_conf_port_before_processing_input() -> None:
    component = SplitString(split_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(common_capnp.StructuredText.new_message(type="toml", value='split_at = ";"')),
                done_message(),
            ],
            "in": [
                ip_message("alpha;beta;gamma\n"),
                done_message(),
            ],
        },
    ).output()

    assert component.config.split_at == ";"
    assert text_outputs(writer) == ["alpha", "beta", "gamma"]


def test_split_string2_reads_unstructured_json_conf_port_before_processing_input() -> None:
    component = SplitString(split_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message('{"split_at": ";"}'),
                done_message(),
            ],
            "in": [
                ip_message("alpha;beta;gamma\n"),
                done_message(),
            ],
        },
    ).output()

    assert component.config.split_at == ";"
    assert text_outputs(writer) == ["alpha", "beta", "gamma"]


def test_to_string_can_start_with_default_config() -> None:
    component = ToString(to_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("alpha"),
                done_message(),
            ],
        },
    ).output()

    assert len(writer.values) == 1


def test_to_string_reads_conf_port_before_processing_input() -> None:
    component = ToString(to_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(
                    common_capnp.StructuredText.new_message(
                        type="toml",
                        value='struct_type = "mas.schema.common.common_capnp:Value"',
                    ),
                ),
                done_message(),
            ],
            "in": [
                ip_message(common_capnp.Value.new_message(t="alpha")),
                done_message(),
            ],
        },
    ).output()

    assert component.config.struct_type == "mas.schema.common.common_capnp:Value"
    assert text_outputs(writer) == ['(t = "alpha")']
