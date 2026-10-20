from __future__ import annotations

import json

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.ip.join_ips_by_key import METADATA, JoinIPsByKey


def keyed(content, key, **attrs):
    ip = fbp_capnp.IP.new_message(content=content)
    all_attrs = {"key": key, **attrs}
    kvs = ip.init("attributes", len(all_attrs))
    for i, (name, value) in enumerate(all_attrs.items()):
        kvs[i].key = name
        kvs[i].value = common_capnp.Value.new_message(t=value)
        kvs[i].valueType = VALUE_TYPE
    return PortMessage(PortValue(ip))


def run(slots, outputs=("out", "unmatched"), **settings):
    ports: dict = {}
    if settings:
        ports["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(
        JoinIPsByKey(METADATA),
        inputs=ports,
        outputs=outputs,
        array_inputs={"in": [[*msgs, done_message()] for msgs in slots]},
    )


def texts(writer):
    return [v.content.as_text() for v in writer.values if str(v.type) == "standard"]


def attrs_of(value):
    return {kv.key: python_from_attr(kv) for kv in value.attributes}


def test_joins_ips_sharing_a_key() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]])
    assert texts(result.output("out")) == ["a1"]
    assert result.output("out").values[0].attributes is not None


def test_the_partner_content_lands_in_an_attribute() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]])
    joined = result.output("out").values[0]
    assert joined.content.as_text() == "a1"
    assert joined.attributes[-1].key == "in1"
    assert joined.attributes[-1].value.as_text() == "b1"


def test_order_of_arrival_does_not_matter() -> None:
    """The whole point: branches that return out of order still pair correctly."""
    result = run([[keyed("a1", "k1"), keyed("a2", "k2")], [keyed("b2", "k2"), keyed("b1", "k1")]])
    joined = {v.content.as_text(): v for v in result.output("out").values}
    assert set(joined) == {"a1", "a2"}
    assert joined["a1"].attributes[-1].value.as_text() == "b1"
    assert joined["a2"].attributes[-1].value.as_text() == "b2"


def test_positional_zipping_would_have_mispaired_these() -> None:
    result = run([[keyed("a1", "k1"), keyed("a2", "k2")], [keyed("b2", "k2"), keyed("b1", "k1")]])
    for value in result.output("out").values:
        primary_key = attrs_of(value)["key"]
        partner = value.attributes[-1].value.as_text()
        assert partner[1:] == primary_key[1:]


def test_three_inputs_all_have_to_arrive() -> None:
    result = run([[keyed("a", "k")], [keyed("b", "k")], [keyed("c", "k")]])
    joined = result.output("out").values[0]
    assert {kv.key for kv in joined.attributes} >= {"in1", "in2"}


def test_an_incomplete_group_goes_to_unmatched_when_the_inputs_close() -> None:
    result = run([[keyed("a1", "k1")], []])
    assert texts(result.output("out")) == []
    assert texts(result.output("unmatched")) == ["a1"]


def test_incomplete_groups_are_dropped_when_unmatched_is_unconnected() -> None:
    result = run([[keyed("a1", "k1")], []], outputs=("out",))
    assert texts(result.output("out")) == []


def test_a_json_object_can_be_built_instead() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]], combine="json_object")
    assert json.loads(result.output("out").values[0].content.as_text()) == {"0": "a1", "1": "b1"}


def test_a_substream_can_be_emitted_instead() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]], combine="substream")
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "standard",
        "closeBracket",
    ]


def test_the_primary_input_is_configurable() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]], primary=1)
    assert result.output("out").values[0].content.as_text() == "b1"


def test_the_key_can_be_recorded_on_the_joined_ip() -> None:
    result = run([[keyed("a1", "k1")], [keyed("b1", "k1")]], key_attr="joined_on")
    assert attrs_of(result.output("out").values[0])["joined_on"] == "k1"


def test_joining_on_a_json_content_path() -> None:
    left = ip_message(json.dumps({"id": 7, "side": "left"}))
    right = ip_message(json.dumps({"id": 7, "side": "right"}))
    result = run([[left], [right]], selector="./id")
    assert len(result.output("out").values) == 1


def test_max_pending_drops_the_oldest_incomplete_group() -> None:
    result = run(
        [[keyed("a1", "k1"), keyed("a2", "k2"), keyed("a3", "k3")], []],
        max_pending=1,
    )
    assert texts(result.output("unmatched")) == ["a1", "a2", "a3"]


def test_a_duplicate_on_one_input_keeps_the_first() -> None:
    result = run([[keyed("a1", "k1"), keyed("a1-again", "k1")], [keyed("b1", "k1")]])
    assert texts(result.output("out")) == ["a1"]


def test_bracket_ips_are_dropped_since_a_join_regroups() -> None:
    result = run([[open_bracket_message(), keyed("a1", "k1")], [keyed("b1", "k1")]])
    assert [str(v.type) for v in result.output("out").values] == ["standard"]
