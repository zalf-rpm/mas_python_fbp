from __future__ import annotations

import json

from tests.component_harness import (
    NO_MSG,
    ReadIfMsgReader,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.json.get_value_by_key import METADATA, Component


def _obj_reader(initial_json: str, if_msg_messages=()):
    return ReadIfMsgReader(read_messages=[ip_message(initial_json)], if_msg_messages=list(if_msg_messages))


def test_looks_up_present_and_missing_keys_with_default_empty_string() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1, "b": "two"}), if_msg_messages=[NO_MSG, NO_MSG]),
            "key": [
                ip_message(json.dumps("a")),
                ip_message(json.dumps("missing")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["1", '""']


def test_missing_key_uses_configured_marker_value() -> None:
    # missing_key_value accepts any JSON value - not just strings - via the conf port, e.g. an
    # object here. (A literal JSON null cannot be set this way: the config layer treats a None
    # override as "no override provided" for every field, not just this one - see
    # apply_config_values. A static null default in the Field(...) declaration itself would still
    # work fine, since that path never goes through apply_config_values' None-filtering.)
    component = Component(METADATA)
    component.apply_config_values({"missing_key_value": {"status": "missing"}})

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1}), if_msg_messages=[NO_MSG]),
            "key": [
                ip_message(json.dumps("missing")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ['{"status": "missing"}']


def test_missing_key_emits_nothing_when_disabled() -> None:
    component = Component(METADATA)
    component.apply_config_values({"emit_message_for_missing_key": False})

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1}), if_msg_messages=[NO_MSG, NO_MSG]),
            "key": [
                ip_message(json.dumps("a")),
                ip_message(json.dumps("missing")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["1"]


def test_refreshes_object_from_obj_port_between_key_lookups() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(
                json.dumps({"a": 1}),
                if_msg_messages=[NO_MSG, ip_message(json.dumps({"a": 99}))],
            ),
            "key": [
                ip_message(json.dumps("a")),
                ip_message(json.dumps("a")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["1", "99"]


def test_stops_checking_obj_once_it_reports_done_and_keeps_last_object() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1}), if_msg_messages=[done_message()]),
            "key": [
                ip_message(json.dumps("a")),
                ip_message(json.dumps("a")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    assert component.in_ports["obj"] is None
    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["1", "1"]


def test_substream_on_obj_is_drained_and_merged_into_one_object() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": ReadIfMsgReader(
                read_messages=[
                    open_bracket_message(),
                    ip_message(json.dumps({"a": 1})),
                    ip_message(json.dumps({"b": 2})),
                    close_bracket_message(),
                ],
                if_msg_messages=[NO_MSG],
            ),
            "key": [
                ip_message(json.dumps("b")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["2"]


def test_bracket_ips_on_key_port_are_forwarded_unchanged() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1}), if_msg_messages=[NO_MSG, NO_MSG]),
            "key": [
                open_bracket_message(),
                ip_message(json.dumps("a")),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    out = result.output("value").values
    assert [ip.type for ip in out] == ["openBracket", "standard", "closeBracket"]
    assert out[1].content.as_text() == "1"


def test_unconnected_obj_port_always_takes_the_missing_key_path() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "key": [
                ip_message(json.dumps("anything")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ['""']


def test_path_separator_resolves_nested_objects_and_list_indices() -> None:
    component = Component(METADATA)
    obj = {"a": {"b": {"c": 42}}, "items": [10, 20, 30]}

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps(obj), if_msg_messages=[NO_MSG, NO_MSG]),
            "key": [
                ip_message(json.dumps("a/b/c")),
                ip_message(json.dumps("items/1")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["42", "20"]


def test_missing_path_segment_takes_the_missing_key_path() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": {"b": 1}}), if_msg_messages=[NO_MSG]),
            "key": [
                ip_message(json.dumps("a/missing/c")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ['""']


def test_empty_path_separator_disables_splitting() -> None:
    component = Component(METADATA)
    component.apply_config_values({"path_separator": ""})

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a/b": 7}), if_msg_messages=[NO_MSG]),
            "key": [
                ip_message(json.dumps("a/b")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["7"]


def test_custom_path_separator() -> None:
    component = Component(METADATA)
    component.apply_config_values({"path_separator": "."})

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": {"b": 5}}), if_msg_messages=[NO_MSG]),
            "key": [
                ip_message(json.dumps("a.b")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    values = [ip.content.as_text() for ip in result.output("value").values]
    assert values == ["5"]


def test_key_substream_shares_one_object_snapshot_taken_at_open_bracket() -> None:
    # Even though a newer object becomes available via readIfMsg partway through, all 'key' IPs
    # inside the substream must still see the object snapshot taken when the substream opened.
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(
                json.dumps({"a": 1}),
                if_msg_messages=[ip_message(json.dumps({"a": 2})), NO_MSG],
            ),
            "key": [
                open_bracket_message(),
                ip_message(json.dumps("a")),
                ip_message(json.dumps("a")),
                close_bracket_message(),
                ip_message(json.dumps("a")),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    out = result.output("value").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
        "standard",
    ]
    # both lookups inside the substream see the object as it was when the substream opened
    assert out[1].content.as_text() == "2"
    assert out[2].content.as_text() == "2"
    # the lookup after the substream closed can observe a further refresh again
    assert out[4].content.as_text() == "2"


def test_nested_key_substream_only_refreshes_once_for_the_whole_substream() -> None:
    component = Component(METADATA)

    result = run_process_component(
        component,
        inputs={
            "obj": _obj_reader(json.dumps({"a": 1}), if_msg_messages=[NO_MSG]),
            "key": [
                open_bracket_message(),
                open_bracket_message(),
                ip_message(json.dumps("a")),
                close_bracket_message(),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("value",),
    )

    out = result.output("value").values
    assert [ip.type for ip in out] == [
        "openBracket",
        "openBracket",
        "standard",
        "closeBracket",
        "closeBracket",
    ]
    assert out[2].content.as_text() == "1"
