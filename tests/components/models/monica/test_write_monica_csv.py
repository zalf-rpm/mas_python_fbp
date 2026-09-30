"""write_monica_csv: narrowed error handling (plan LP5)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
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
)
from zalfmas_fbp.components.models.monica.write_monica_csv import METADATA, Component


def oid(name, *, unit="", organ=0, layer=0):
    """One MONICA output id. monica_io indexes these directly, so every key it reads must exist."""

    return {
        "name": name,
        "displayName": "",
        "unit": unit,
        "organ": organ,
        "layerAggOp": 0,
        "timeAggOp": 0,
        "fromLayer": layer,
        "toLayer": layer,
        "jsonInput": name,
    }


RESULT = {
    "data": [
        {
            "origSpec": '"daily"',
            "outputIds": [
                oid("Date"),
                oid("Yield", unit="kg"),
            ],
            "results": [["2020-01-01", "2020-01-02"], [1.0, 2.0]],
        }
    ]
}


def result_ip(payload=None, *, to_attr=None, **attrs):
    st = common_capnp.StructuredText.new_message(type="json", value=json.dumps(RESULT if payload is None else payload))
    ip = fbp_capnp.IP.new_message()
    named = dict(attrs)
    if to_attr:
        named[to_attr] = st
    else:
        ip.content = st
    if named:
        entries = ip.init("attributes", len(named))
        for i, (key, value) in enumerate(named.items()):
            entries[i].key = key
            entries[i].value = value
    return PortMessage(PortValue(ip))


def run(messages, tmp_path, **settings):
    settings.setdefault("path_to_out_dir", str(tmp_path))
    inputs: dict = {
        "in": [*messages, done_message()],
        "conf": [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ],
    }
    return run_process_component(Component(METADATA), inputs=inputs, outputs=())


def written(tmp_path) -> list[Path]:
    return sorted(p for p in Path(tmp_path).iterdir() if p.is_file())


def test_it_writes_a_csv(tmp_path) -> None:
    run([result_ip()], tmp_path)
    files = written(tmp_path)
    assert len(files) == 1
    text = files[0].read_text()
    assert "daily" in text
    assert "Yield" in text


def test_one_file_per_ip(tmp_path) -> None:
    """A pattern that cannot be resolved falls back to the running IP count, so the files differ.
    With the default constant-ish pattern both IPs would write the same path."""

    run([result_ip(), result_ip()], tmp_path, filepath_pattern="out_{no_such_key}.csv")
    assert [p.name for p in written(tmp_path)] == ["out_1.csv", "out_2.csv"]


def test_brackets_do_not_produce_a_file(tmp_path) -> None:
    run([open_bracket_message(), result_ip(), close_bracket_message()], tmp_path)
    assert len(written(tmp_path)) == 1


def test_a_payload_that_is_not_json_is_skipped(tmp_path) -> None:
    run([ip_message("not json"), result_ip()], tmp_path)
    assert len(written(tmp_path)) == 1


def test_an_unreadable_payload_can_fail_the_process(tmp_path) -> None:
    with pytest.raises(ValueError, match="no MONICA result"):
        run([ip_message("not json")], tmp_path, on_error="fail")


def test_from_attr_reads_the_result_out_of_an_attribute(tmp_path) -> None:
    run([result_ip(to_attr="results")], tmp_path, from_attr="results")
    assert len(written(tmp_path)) == 1


def test_a_result_without_data_writes_an_empty_file(tmp_path) -> None:
    run([result_ip({"data": []})], tmp_path)
    assert len(written(tmp_path)) == 1


def test_the_output_directory_is_created(tmp_path) -> None:
    target = tmp_path / "nested" / "deeper"
    run([result_ip()], tmp_path, path_to_out_dir=str(target))
    assert len(written(target)) == 1


def test_a_fault_in_the_component_is_not_swallowed(tmp_path, monkeypatch) -> None:
    """A `try` around the whole per-IP body meant a component fault logged a traceback per IP
    while the flow quietly wrote nothing."""

    def boom(self, in_ip):
        msg = "a fault inside the component"
        raise RuntimeError(msg)

    monkeypatch.setattr(Component, "results_of", boom)
    with pytest.raises(RuntimeError, match="a fault inside the component"):
        run([result_ip()], tmp_path)
