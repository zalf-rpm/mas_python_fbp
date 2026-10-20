"""split_json -> filter -> group -> reduce, run as a pipeline.

The plan asks for this: the substream components are only useful if they compose, and each one's
output has to be something the next actually accepts.
"""

from __future__ import annotations

import json

from mas.schema.common import common_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    done_message,
    ip_message,
    run_process_component,
)
from zalfmas_fbp.components.common.values import python_from_attr, python_from_value
from zalfmas_fbp.components.ip.filter_ips import METADATA as FILTER_META
from zalfmas_fbp.components.ip.filter_ips import FilterIPs
from zalfmas_fbp.components.ip.group_into_substreams import METADATA as GROUP_META
from zalfmas_fbp.components.ip.group_into_substreams import GroupIntoSubstreams
from zalfmas_fbp.components.ip.reduce_substream import METADATA as REDUCE_META
from zalfmas_fbp.components.ip.reduce_substream import ReduceSubstream
from zalfmas_fbp.components.json.split_json import METADATA as SPLIT_META
from zalfmas_fbp.components.json.split_json import SplitJson

ROWS = [
    {"site": "north", "yield": 7.0},
    {"site": "north", "yield": 5.0},
    {"site": "south", "yield": 9.0},
    {"site": "south", "yield": 1.0},
    {"site": "east", "yield": 2.0},
]


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def stage(component, upstream, **settings):
    """Feed one component the IPs another produced, and return what it emits."""
    inputs = {"in": [PortMessage(PortValue(v)) for v in upstream] + [done_message()]}
    if settings:
        inputs["conf"] = conf(**settings)
    return run_process_component(component, inputs=inputs, outputs=("out",)).output().values


def test_split_filter_group_reduce() -> None:
    document = ip_message(json.dumps({"run": "r1", "rows": ROWS}))

    split = (
        run_process_component(
            SplitJson(SPLIT_META),
            inputs={
                "in": [document, done_message()],
                "conf": conf(traversal_path="rows", wrap_in_substream=False, copy_parent_paths={"run": "run"}),
            },
            outputs=("out",),
        )
        .output()
        .values
    )
    assert len(split) == len(ROWS)

    kept = stage(
        FilterIPs(FILTER_META),
        split,
        predicate={"left": "./yield", "op": "ge", "right": 2.0},
    )
    assert len(kept) == 4  # the south row at 1.0 is the only one below the threshold

    grouped = stage(GroupIntoSubstreams(GROUP_META), kept, selector="./site", mode="buffer_all")

    reduced = stage(
        ReduceSubstream(REDUCE_META),
        grouped,
        aggregations=[
            {"selector": "./yield", "op": "mean", "to_attr": "avg_yield", "to_content": True},
            {"op": "count", "to_attr": "rows"},
        ],
    )

    by_site = {python_from_attr(next(kv for kv in ip.attributes if kv.key == "group_key")): ip for ip in reduced}
    assert set(by_site) == {"north", "south", "east"}
    assert python_from_value(by_site["north"].content.as_struct(common_capnp.Value)) == 6.0
    assert python_from_value(by_site["south"].content.as_struct(common_capnp.Value)) == 9.0
    assert python_from_attr(next(kv for kv in by_site["north"].attributes if kv.key == "rows")) == 2


def test_the_parent_field_survives_every_stage() -> None:
    """An attribute attached at the split is still there after grouping and reduction."""
    document = ip_message(json.dumps({"run": "r1", "rows": ROWS[:2]}))

    split = (
        run_process_component(
            SplitJson(SPLIT_META),
            inputs={
                "in": [document, done_message()],
                "conf": conf(traversal_path="rows", wrap_in_substream=False, copy_parent_paths={"run": "run"}),
            },
            outputs=("out",),
        )
        .output()
        .values
    )

    grouped = stage(GroupIntoSubstreams(GROUP_META), split, selector="@run", mode="buffer_all")
    reduced = stage(ReduceSubstream(REDUCE_META), grouped, aggregations=[{"op": "count", "to_attr": "n"}])

    assert len(reduced) == 1
    attrs = {kv.key: python_from_attr(kv) for kv in reduced[0].attributes}
    assert attrs["group_key"] == "r1"
    assert attrs["n"] == 2


def test_split_into_substreams_feeds_reduce_directly() -> None:
    """split_json's own substream wrapper is the grouping, so no group stage is needed."""
    document = ip_message(json.dumps(ROWS))

    split = (
        run_process_component(
            SplitJson(SPLIT_META),
            inputs={"in": [document, done_message()]},
            outputs=("out",),
        )
        .output()
        .values
    )

    reduced = stage(
        ReduceSubstream(REDUCE_META),
        split,
        aggregations=[{"selector": "./yield", "op": "sum", "to_attr": "total", "to_content": True}],
    )
    assert python_from_value(reduced[0].content.as_struct(common_capnp.Value)) == 24.0
