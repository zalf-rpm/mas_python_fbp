from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from zalfmas_fbp.run.run_fbp_flow import (
    FlowArgs,
    FlowLink,
    FlowRunner,
    PortRef,
    decode_chan_id,
    load_cmds,
    parse_config,
    parse_flow,
)

SPLIT_STRING2_ID = "d44040ab-7d5a-44d1-94e8-3f79969edbd4"
CONSOLE_OUTPUT_ID = "2de9c491-d8a6-4b36-84de-db7f4a312731"
COPY_IP_ID = "b1e875af-4ee7-4937-8824-17d185216ec4"


def _metadata(component_id: str, name: str, component_type: str, out_ports: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "info": {"id": component_id, "name": name},
        "type": component_type,
        "inPorts": [{"name": "in"}, {"name": "conf"}],
        "outPorts": out_ports,
    }


@pytest.fixture
def flow_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    cmds = {
        "id": "some-cmds-file-id",
        "name": "test cmds",
        "___disabled": "python -m nope",
        SPLIT_STRING2_ID: "python -m zalfmas_fbp.components.string.split_string2",
        CONSOLE_OUTPUT_ID: "python -m zalfmas_fbp.components.console.console_output",
        COPY_IP_ID: "python -m zalfmas_fbp.components.ip.copy_ip",
    }
    cmds_path = tmp_path / "cmds.json"
    _ = cmds_path.write_text(json.dumps(cmds))

    components = {
        SPLIT_STRING2_ID: _metadata(SPLIT_STRING2_ID, "split string2", "process", [{"name": "out"}]),
        CONSOLE_OUTPUT_ID: _metadata(CONSOLE_OUTPUT_ID, "output to console", "standard", []),
        COPY_IP_ID: _metadata(COPY_IP_ID, "copy IP", "standard", [{"name": "out", "type": "array"}]),
    }
    components_path = tmp_path / "components.json"
    _ = components_path.write_text(json.dumps(components))

    flow = {
        "nodes": [
            {"nodeId": "iip-1", "componentId": "iip", "content": "a;b;c"},
            {
                "nodeId": "split-1",
                "processName": "split 1",
                "componentId": SPLIT_STRING2_ID,
                "config": {"split_at": ";"},
            },
            {"nodeId": "copy-1", "processName": "copy 1", "componentId": COPY_IP_ID, "config": {"count": 2}},
            {"nodeId": "console-1", "processName": "console 1", "componentId": CONSOLE_OUTPUT_ID},
            {"nodeId": "console-2", "processName": "console 2", "componentId": CONSOLE_OUTPUT_ID},
        ],
        "links": [
            {"source": {"nodeId": "iip-1", "port": "Right"}, "target": {"nodeId": "split-1", "port": "in"}},
            {"source": {"nodeId": "split-1", "port": "out"}, "target": {"nodeId": "copy-1", "port": "in"}},
            {"source": {"nodeId": "copy-1", "port": "out"}, "target": {"nodeId": "console-1", "port": "in"}},
            {"source": {"nodeId": "copy-1", "port": "out"}, "target": {"nodeId": "console-2", "port": "in"}},
        ],
    }
    flow_path = tmp_path / "flow.json"
    _ = flow_path.write_text(json.dumps(flow))
    return flow_path, cmds_path, components_path


def _runner(flow_files: tuple[Path, Path, Path]) -> FlowRunner:
    flow_path, cmds_path, components_path = flow_files
    runner = FlowRunner(
        FlowArgs(
            path_to_flow=str(flow_path),
            cmds=[str(cmds_path)],
            components=[str(components_path)],
        ),
    )
    runner.load_flow()
    return runner


def test_chan_id_round_trip():
    link = FlowLink(src=PortRef("a node", "out"), tgt=PortRef("b node", "in"))
    assert decode_chan_id(link.chan_id) == link


def test_parse_config_accepts_dict_toml_and_json():
    assert parse_config({"a": 1}) == {"a": 1}
    assert parse_config('split_at = ";"') == {"split_at": ";"}
    assert parse_config('{"split_at": ";"}') == {"split_at": ";"}
    assert parse_config(None) is None
    assert parse_config("   ") is None


def test_load_cmds_last_file_wins_and_meta_keys_skipped(tmp_path: Path):
    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    _ = first.write_text(json.dumps({"id": "x", "name": "y", "___off": "no", "c1": "cmd a", "c2": "cmd 2"}))
    _ = second.write_text(json.dumps({"c1": "cmd b"}))

    cmds = load_cmds([str(first), str(second)])

    assert cmds == {"c1": "cmd b", "c2": "cmd 2"}


def test_parse_flow_accepts_snake_case_keys():
    nodes, links = parse_flow(
        {
            "nodes": [
                {"node_id": "n1", "process_name": "node 1", "component_id": "c1", "parallel_processes": 3},
                {"node_id": "n2", "component_id": "iip", "content": "hello"},
            ],
            "links": [{"source": {"node_id": "n2", "port": "Right"}, "target": {"node_id": "n1", "port": "in"}}],
        },
    )

    assert nodes["n1"].name == "node 1"
    assert nodes["n1"].parallel_count == 3
    assert nodes["n2"].content == "hello"
    assert links == [FlowLink(src=PortRef("n2", "Right"), tgt=PortRef("n1", "in"))]


def test_parse_flow_drops_links_to_unknown_nodes():
    _nodes, links = parse_flow(
        {
            "nodes": [{"nodeId": "n1"}],
            "links": [{"source": {"nodeId": "n1", "port": "out"}, "target": {"nodeId": "gone", "port": "in"}}],
        },
    )

    assert links == []


def test_load_flow_classifies_nodes_by_metadata(flow_files: tuple[Path, Path, Path]):
    runner = _runner(flow_files)

    assert runner.nodes["split-1"].kind == "process"
    assert runner.nodes["copy-1"].kind == "standard"
    assert runner.nodes["console-1"].kind == "standard"
    assert runner.nodes["iip-1"].kind == "iip"
    assert runner.nodes["split-1"].cmd == "python -m zalfmas_fbp.components.string.split_string2"


def test_config_iips_are_generated_only_for_standard_components(flow_files: tuple[Path, Path, Path]):
    runner = _runner(flow_files)

    conf_links = [link for link in runner.links if link.tgt.port == "conf"]
    assert [link.tgt.node_id for link in conf_links] == ["copy-1"]

    iip_node = runner.nodes[conf_links[0].src.node_id]
    assert iip_node.kind == "iip"
    assert iip_node.content == {"count": 2}

    # the process component keeps its config, it is applied via setConfigEntry
    assert runner.nodes["split-1"].config == {"split_at": ";"}


def test_no_config_iip_when_conf_port_is_connected(tmp_path: Path, flow_files: tuple[Path, Path, Path]):
    flow_path, cmds_path, components_path = flow_files
    flow = json.loads(flow_path.read_text())
    flow["nodes"].append({"nodeId": "iip-conf", "componentId": "iip", "content": "count = 5"})
    flow["links"].append(
        {"source": {"nodeId": "iip-conf", "port": "Right"}, "target": {"nodeId": "copy-1", "port": "conf"}},
    )
    connected_flow_path = tmp_path / "connected_flow.json"
    _ = connected_flow_path.write_text(json.dumps(flow))

    runner = FlowRunner(
        FlowArgs(
            path_to_flow=str(connected_flow_path),
            cmds=[str(cmds_path)],
            components=[str(components_path)],
        ),
    )
    runner.load_flow()

    assert runner.nodes["copy-1"].config_is_connected
    conf_links = [link for link in runner.links if link.tgt.port == "conf"]
    assert [link.src.node_id for link in conf_links] == ["iip-conf"]


def test_load_flow_raises_without_a_command(tmp_path: Path):
    flow_path = tmp_path / "flow.json"
    _ = flow_path.write_text(json.dumps({"nodes": [{"nodeId": "n1", "componentId": "unknown-id"}], "links": []}))
    cmds_path = tmp_path / "cmds.json"
    _ = cmds_path.write_text(json.dumps({}))

    runner = FlowRunner(FlowArgs(path_to_flow=str(flow_path), cmds=[str(cmds_path)]))

    with pytest.raises(RuntimeError, match="No command found"):
        runner.load_flow()


def test_port_infos_message_uses_srs_list_for_array_out_ports(flow_files: tuple[Path, Path, Path]):
    runner = _runner(flow_files)
    runner.in_srs["copy-1"]["in"].append("reader-sr")
    runner.out_srs["copy-1"]["out"].extend(["writer-sr-1", "writer-sr-2"])

    port_infos = runner.port_infos(runner.nodes["copy-1"])

    assert port_infos["inPorts"] == [{"name": "in", "sr": "reader-sr"}]
    assert port_infos["outPorts"] == [{"name": "out", "srs": ["writer-sr-1", "writer-sr-2"]}]


def test_port_infos_message_uses_single_sr_for_standard_out_port(flow_files: tuple[Path, Path, Path]):
    runner = _runner(flow_files)
    runner.nodes["copy-1"].metadata.outPorts[0].type = None
    runner.out_srs["copy-1"]["out"].append("writer-sr-1")

    port_infos = runner.port_infos(runner.nodes["copy-1"])

    assert port_infos["outPorts"] == [{"name": "out", "sr": "writer-sr-1"}]
