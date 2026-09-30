"""add_attribute / add_content, migrated off copy_and_set_fbp_attrs (plan LP5).

Bracket behaviour is covered in test_attribute_component_brackets.py; this covers what the two
actually do with attributes and content, which nothing pinned before.

The upstream `copy_and_set_fbp_attrs` dropped `desc`, applied only the *first* override it found
(it `break`s out of the scan), and wrote overrides with no `valueType` - so an attribute these
components attached could not be read back.
"""

from __future__ import annotations

import json

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.geo import geo_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import VALUE_TYPE, python_from_attr
from zalfmas_fbp.components.ip.add_attribute import METADATA as ADD_ATTR_META
from zalfmas_fbp.components.ip.add_attribute import Component as AddAttribute
from zalfmas_fbp.components.ip.add_content import METADATA as ADD_CONTENT_META
from zalfmas_fbp.components.ip.add_content import Component as AddContent


def ip_with(content=None, content_type=None, attrs=None, descs=None):
    ip = fbp_capnp.IP.new_message()
    if content is not None:
        ip.content = content
    if content_type:
        ip.sysAttributes.contentType = content_type
    if attrs:
        entries = ip.init("attributes", len(attrs))
        for i, (key, value) in enumerate(attrs.items()):
            entries[i].key = key
            entries[i].value = common_capnp.Value.new_message(t=value)
            entries[i].valueType = VALUE_TYPE
            if descs and key in descs:
                entries[i].desc = descs[key]
    return PortMessage(PortValue(ip))


def attrs_of(ip) -> dict:
    return {kv.key: kv for kv in ip.attributes}


def conf(**settings):
    return [
        PortMessage(
            PortValue(
                fbp_capnp.IP.new_message(
                    content=common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))
                )
            )
        ),
        done_message(),
    ]


def run_add_attribute(in_msgs, attr_msgs, to_attr="added"):
    return run_process_component(
        AddAttribute(ADD_ATTR_META),
        inputs={
            "in": [*in_msgs, done_message()],
            "attr": [*attr_msgs, done_message()],
            "conf": conf(to_attr=to_attr),
        },
        outputs=("out",),
    )


def run_add_content(in_msgs, content_msgs, to_attr=""):
    return run_process_component(
        AddContent(ADD_CONTENT_META),
        inputs={
            "in": [*in_msgs, done_message()],
            "content": [*content_msgs, done_message()],
            "conf": conf(to_attr=to_attr),
        },
        outputs=("out",),
    )


# --- add_attribute ---------------------------------------------------------------------------


def test_add_attribute_attaches_the_incoming_content_as_an_attribute() -> None:
    result = run_add_attribute([ip_with("payload")], [ip_with("the-value")])
    kv = attrs_of(result.output("out").values[0])["added"]
    assert kv.value.as_text() == "the-value"


def test_add_attribute_tags_the_attribute_with_the_sources_content_type() -> None:
    """Without a valueType the attribute resolves to MISSING downstream (D4), so what this
    component attached could not be read back at all."""

    coord = geo_capnp.LatLonCoord.new_message(lat=52.0, lon=13.0)
    result = run_add_attribute([ip_with("payload")], [ip_with(coord, "geo.capnp:LatLonCoord")])
    kv = attrs_of(result.output("out").values[0])["added"]
    assert kv.valueType == "geo.capnp:LatLonCoord"
    assert round(kv.value.as_struct(geo_capnp.LatLonCoord).lat, 3) == 52.0


def test_add_attribute_keeps_the_incoming_attributes() -> None:
    result = run_add_attribute([ip_with("payload", attrs={"kept": "yes"})], [ip_with("v")])
    assert sorted(attrs_of(result.output("out").values[0])) == ["added", "kept"]


def test_add_attribute_preserves_an_attributes_description() -> None:
    """`copy_and_set_fbp_attrs` dropped `desc`."""

    result = run_add_attribute(
        [ip_with("payload", attrs={"kept": "yes"}, descs={"kept": "why it is here"})],
        [ip_with("v")],
    )
    assert attrs_of(result.output("out").values[0])["kept"].desc == "why it is here"


def test_add_attribute_overrides_an_attribute_of_the_same_name() -> None:
    result = run_add_attribute([ip_with("payload", attrs={"added": "old"})], [ip_with("new")])
    kv = attrs_of(result.output("out").values[0])["added"]
    assert kv.value.as_text() == "new"


def test_add_attribute_reuses_the_last_attr_for_later_ips() -> None:
    """The 'attr' port closing used to take the IP read in that same turn with it, so one input
    was silently lost every time the attr side finished first."""

    result = run_add_attribute([ip_with("a"), ip_with("b")], [ip_with("once")])
    values = result.output("out").values
    assert [v.content.as_text() for v in values] == ["a", "b"]
    assert [attrs_of(v)["added"].value.as_text() for v in values] == ["once", "once"]


# --- add_content -----------------------------------------------------------------------------


def test_add_content_replaces_the_content() -> None:
    result = run_add_content([ip_with("old")], [ip_with("new")])
    assert result.output("out").values[0].content.as_text() == "new"


def test_add_content_carries_the_new_contents_type() -> None:
    coord = geo_capnp.LatLonCoord.new_message(lat=1.0, lon=2.0)
    result = run_add_content([ip_with("old")], [ip_with(coord, "geo.capnp:LatLonCoord")])
    assert result.output("out").values[0].sysAttributes.contentType == "geo.capnp:LatLonCoord"


def test_add_content_keeps_the_incoming_attributes() -> None:
    result = run_add_content([ip_with("old", attrs={"kept": "yes"})], [ip_with("new")])
    kv = attrs_of(result.output("out").values[0])["kept"]
    assert python_from_attr(kv) == "yes"


def test_add_content_preserves_an_attributes_description() -> None:
    result = run_add_content(
        [ip_with("old", attrs={"kept": "yes"}, descs={"kept": "why"})],
        [ip_with("new")],
    )
    assert attrs_of(result.output("out").values[0])["kept"].desc == "why"


def test_to_attr_keeps_the_displaced_content_with_its_own_type() -> None:
    """The content being pushed aside is only readable later if its type travels with it."""

    coord = geo_capnp.LatLonCoord.new_message(lat=52.0, lon=13.0)
    result = run_add_content(
        [ip_with(coord, "geo.capnp:LatLonCoord")],
        [ip_with("new")],
        to_attr="was_content",
    )
    kv = attrs_of(result.output("out").values[0])["was_content"]
    assert kv.valueType == "geo.capnp:LatLonCoord"
    assert round(kv.value.as_struct(geo_capnp.LatLonCoord).lon, 3) == 13.0


def test_without_to_attr_the_old_content_is_simply_dropped() -> None:
    result = run_add_content([ip_with("old")], [ip_with("new")])
    assert list(result.output("out").values[0].attributes) == []


def test_add_content_reuses_the_last_content_for_later_ips() -> None:
    """Same as add_attribute: the 'content' port closing used to lose an input IP."""

    result = run_add_content([ip_with("a"), ip_with("b")], [ip_with("new")])
    assert [v.content.as_text() for v in result.output("out").values] == ["new", "new"]
