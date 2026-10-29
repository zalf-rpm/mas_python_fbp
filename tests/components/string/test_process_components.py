from __future__ import annotations

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
    text_outputs,
)
from zalfmas_fbp.components.string.split_string import METADATA as split_string_metadata
from zalfmas_fbp.components.string.split_string import SplitString
from zalfmas_fbp.components.string.to_string import METADATA as to_string_metadata
from zalfmas_fbp.components.string.to_string import ToString

STRUCTURED_TEXT_CONTENT_TYPE = "@0xed6c098b67cad454 = common/common.capnp:StructuredText"
VALUE_CONTENT_TYPE = "@0xe17592335373b246 = common/common.capnp:Value"


def test_split_string_uses_default_config_and_writes_split_values() -> None:
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


def test_split_string_reads_conf_port_before_processing_input() -> None:
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


def test_split_string_reads_unstructured_json_conf_port_before_processing_input() -> None:
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


def test_to_string_falls_back_to_repr_for_an_unresolvable_content_type() -> None:
    """Characterization (plan D12): an unparseable type must not raise, it degrades to str()."""
    component = ToString(to_string_metadata)
    in_ip = fbp_capnp.IP.new_message(
        content=common_capnp.Value.new_message(t="alpha"),
        sysAttributes={"contentType": "not a content type"},
    )

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
            "in": [PortMessage(PortValue(in_ip)), done_message()],
        },
    ).output()

    assert text_outputs(writer) == ['(t = "alpha")']


def test_to_string_without_any_usable_type_uses_the_raw_representation() -> None:
    component = ToString(to_string_metadata)

    writer = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(common_capnp.StructuredText.new_message(type="toml", value="struct_type = ''")),
                done_message(),
            ],
            "in": [ip_message(common_capnp.Value.new_message(t="alpha")), done_message()],
        },
    ).output()

    assert len(writer.values) == 1
    assert "alpha" not in text_outputs(writer)[0]


# --- split_string and substreams (plan LP4) ---------------------------------------------------


def _split_conf(**settings):
    import json

    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def _run_split(messages, **settings):
    from zalfmas_fbp.components.string.split_string import METADATA as split_meta
    from zalfmas_fbp.components.string.split_string import SplitString

    inputs: dict = {"in": [*messages, done_message()]}
    if settings:
        inputs["conf"] = _split_conf(**settings)
    return run_process_component(SplitString(split_meta), inputs=inputs, outputs=("out",)).output()


def _shapes(writer):
    return [str(v.type) for v in writer.values]


def _parts(writer):
    """Only the split parts: text_outputs would also read the brackets, whose content is empty."""
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def test_split_string_emits_a_flat_stream_by_default() -> None:
    """The parts of successive inputs look the same as strings that arrived separately."""
    writer = _run_split([ip_message("a,b"), ip_message("c,d")])
    assert text_outputs(writer) == ["a", "b", "c", "d"]
    assert _shapes(writer) == ["standard"] * 4


def test_split_string_can_wrap_each_input_in_its_own_substream() -> None:
    writer = _run_split([ip_message("a,b"), ip_message("c,d")], wrap_in_substream=True)
    assert _shapes(writer) == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
    ]
    assert _parts(writer) == ["a", "b", "c", "d"]


def test_split_string_forwards_an_incoming_substream_unchanged() -> None:
    """The caller's grouping is theirs; it survives whatever this component does inside it."""
    writer = _run_split([open_bracket_message(), ip_message("a,b"), close_bracket_message()])
    assert _shapes(writer) == ["openBracket", "standard", "standard", "closeBracket"]


def test_wrapping_nests_inside_an_incoming_substream() -> None:
    writer = _run_split(
        [open_bracket_message(), ip_message("a,b"), close_bracket_message()],
        wrap_in_substream=True,
    )
    assert _shapes(writer) == [
        "openBracket",
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
        "closeBracket",
    ]


def test_split_string_carries_attributes_onto_every_part() -> None:
    from mas.schema.fbp import fbp_capnp

    from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr

    ip = fbp_capnp.IP.new_message(content="a,b")
    kvs = ip.init("attributes", 1)
    kvs[0].key = "region"
    kvs[0].value = common_capnp.Value.new_message(t="north")
    kvs[0].valueType = VALUE_TYPE

    writer = _run_split([PortMessage(PortValue(ip))])
    assert [python_from_attr(v.attributes[0]) for v in writer.values] == ["north", "north"]


def test_empty_parts_are_kept_by_default_and_can_be_dropped() -> None:
    assert text_outputs(_run_split([ip_message("a,,b")])) == ["a", "", "b"]
    assert text_outputs(_run_split([ip_message("a,,b")], keep_empty=False)) == ["a", "b"]
