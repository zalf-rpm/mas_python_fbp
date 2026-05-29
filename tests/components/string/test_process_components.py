from __future__ import annotations

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    run_process_component,
    text_outputs,
)
from zalfmas_fbp.components.string.split_string2 import METADATA as split_string_metadata
from zalfmas_fbp.components.string.split_string2 import SplitString
from zalfmas_fbp.components.string.to_string import METADATA as to_string_metadata
from zalfmas_fbp.components.string.to_string import ToString

STRUCTURED_TEXT_CONTENT_TYPE = "@0xed6c098b67cad454 = common/common.capnp:StructuredText"
VALUE_CONTENT_TYPE = "@0xe17592335373b246 = common/common.capnp:Value"


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
                ip_message(common_capnp.StructuredText.new_message(type="json", value='"alpha"')),
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
                        value=f'struct_type = "{VALUE_CONTENT_TYPE}"',
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

    assert component.config.struct_type == VALUE_CONTENT_TYPE
    assert text_outputs(writer) == ['(t = "alpha")']


def test_to_string_uses_incoming_sys_content_type_before_config() -> None:
    component = ToString(to_string_metadata)
    in_ip = fbp_capnp.IP.new_message(
        content=common_capnp.Value.new_message(t="alpha"),
        sysAttributes={"contentType": VALUE_CONTENT_TYPE},
    )

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(
                    common_capnp.StructuredText.new_message(
                        type="toml",
                        value=f'struct_type = "{STRUCTURED_TEXT_CONTENT_TYPE}"',
                    ),
                ),
                done_message(),
            ],
            "in": [
                PortMessage(PortValue(in_ip)),
                done_message(),
            ],
        },
    ).output()

    assert text_outputs(writer) == ['(t = "alpha")']
