from __future__ import annotations

import json

from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message_with_attrs,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.ip.attribute_to_content import METADATA, Component


def test_extracts_plain_text_attribute_as_content() -> None:
    component = Component(METADATA)
    component.apply_config_values({"attr_name": "name"})

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", name="hello"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert len(out) == 1
    assert out[0].content.as_text() == "hello"


def test_missing_attribute_is_skipped_with_no_output() -> None:
    component = Component(METADATA)
    component.apply_config_values({"attr_name": "missing"})

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", other="x"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    assert result.output("out").values == []


def test_original_attributes_are_preserved_on_output() -> None:
    component = Component(METADATA)
    component.apply_config_values({"attr_name": "name"})

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", name="hello", other="kept"),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out_ip = result.output("out").values[0]
    attr_names = [kv.key for kv in out_ip.attributes]
    assert "name" in attr_names
    assert "other" in attr_names


def test_bracket_ips_are_forwarded_unchanged() -> None:
    component = Component(METADATA)
    component.apply_config_values({"attr_name": "name"})

    result = run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message_with_attrs("ignored", name="hello"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert [ip.type for ip in out] == ["openBracket", "standard", "closeBracket"]
    assert out[1].content.as_text() == "hello"


def test_sub_access_into_common_value_int_field_is_json_encoded() -> None:
    component = Component(METADATA)
    component.apply_config_values(
        {
            "attr_name": "myattr",
            "attr_path": "i64",
            "types": {"@myattr": "@0xe17592335373b246 = common/common.capnp:Value"},
        }
    )

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=common_capnp.Value.new_message(i64=42)),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert len(out) == 1
    assert out[0].content.as_text() == "42"


def test_sub_access_without_configured_type_is_skipped() -> None:
    component = Component(METADATA)
    component.apply_config_values({"attr_name": "myattr", "attr_path": "i64"})

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=common_capnp.Value.new_message(i64=42)),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    assert result.output("out").values == []


def test_sub_access_decodes_embedded_json_and_walks_into_it() -> None:
    component = Component(METADATA)
    component.apply_config_values(
        {
            "attr_name": "myattr",
            "attr_path": "a/b",
            "types": {"@myattr": "@0xed6c098b67cad454 = common/common.capnp:StructuredText"},
        }
    )

    st = common_capnp.StructuredText.new_message(type="json", value=json.dumps({"a": {"b": 5}}))

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=st),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert len(out) == 1
    assert out[0].content.as_text() == "5"


def test_sub_access_missing_path_segment_is_skipped() -> None:
    component = Component(METADATA)
    component.apply_config_values(
        {
            "attr_name": "myattr",
            "attr_path": "a/missing",
            "types": {"@myattr": "@0xed6c098b67cad454 = common/common.capnp:StructuredText"},
        }
    )

    st = common_capnp.StructuredText.new_message(type="json", value=json.dumps({"a": {"b": 5}}))

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=st),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    assert result.output("out").values == []


def test_per_segment_type_ref_recasts_an_untyped_anypointer_field() -> None:
    # Pair's fields are generic (AnyPointer at the dynamic-access level), so reaching 'fst' alone
    # isn't enough to read its 'i64' field - the ':@val' suffix must recast it to Value first.
    component = Component(METADATA)
    component.apply_config_values(
        {
            "attr_name": "myattr",
            "attr_path": "fst:@val/i64",
            "types": {
                "@myattr": "@0xb9d4864725174733 = common/common.capnp:Pair",
                "@val": "@0xe17592335373b246 = common/common.capnp:Value",
            },
        }
    )

    pair = common_capnp.Pair.new_message(fst=common_capnp.Value.new_message(i64=99), snd="ignored")

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=pair),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    out = result.output("out").values
    assert len(out) == 1
    assert out[0].content.as_text() == "99"


def test_per_segment_type_ref_missing_from_types_is_skipped() -> None:
    component = Component(METADATA)
    component.apply_config_values(
        {
            "attr_name": "myattr",
            "attr_path": "fst:@val/i64",
            "types": {"@myattr": "@0xb9d4864725174733 = common/common.capnp:Pair"},
        }
    )

    pair = common_capnp.Pair.new_message(fst=common_capnp.Value.new_message(i64=99), snd="ignored")

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message_with_attrs("ignored", myattr=pair),
                done_message(),
            ],
        },
        outputs=("out",),
    )

    assert result.output("out").values == []
