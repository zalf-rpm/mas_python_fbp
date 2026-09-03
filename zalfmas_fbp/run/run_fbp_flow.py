# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
"""Standalone starter for FBP flows exported by the Blazor draw tool.

In contrast to :mod:`zalfmas_fbp.run.run_blazor_draw_fbp_flow` this version can run flows
which mix old ``standard`` (Runnable) components and new ``process`` components:

* ``standard`` components are started with a sturdy ref to a config channel on which they
  receive a single ``fbp.capnp:PortInfos`` message (the old behaviour).
* ``process`` components are started with a sturdy ref to a ``Channel.Writer`` served by
  this starter. They write their ``fbp.capnp:Process`` capability into it, and the starter
  then calls ``connectInPort``/``connectOutPort``/``setConfigEntry``/``start`` directly.

The component ids are mapped to commands via one or more ``local_cmds.json`` files given
on the command line (``--cmds a.json b.json``). If the same component id appears in several
files, the entry of the last file wins.

Instead of repeating ``--`` flags, an environment can also be captured in one TOML file
passed via ``--config``/``-e`` (a flat table using the same option names, e.g. ``cmds``,
``path_to_channel``, ``host``, ...). Explicit ``--`` flags on the command line still take
precedence over the TOML file's values, so e.g. ``run_fbp_flow.py my_flow.json -e env.toml``
only needs the flow and the environment file, while occasional overrides still work.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import logging
import re
import socket
import subprocess as sp
import sys
import tomllib
import urllib.parse as urlp
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import ValidationError
from zalfmas_common import common

import zalfmas_fbp.run.components as comp
from zalfmas_fbp.run import channels as chans
from zalfmas_fbp.run import process as proc
from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging
from zalfmas_fbp.run.metadata import ComponentMetadata
from zalfmas_fbp.run.process.config.config_codec import config_value_from_python

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ProcessClient, ReaderClient, WriterClient
    from mas.schema.persistence.persistence_capnp.types.builders import SturdyRefBuilder
    from mas.schema.persistence.persistence_capnp.types.readers import SturdyRefReader

logger = logging.getLogger(__name__)
configure_logging(default_level="INFO")

IIP_COMPONENT_ID = "iip"
DEFAULT_PATH_TO_CHANNEL = "./binaries/channel"
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}
PROCESS_CAP_TIMEOUT_SECONDS = 60.0
PROCESS_STOP_TIMEOUT_SECONDS = 10.0
PROCESS_EXIT_POLL_SECONDS = 0.2

type NodeKind = Literal["iip", "standard", "process"]
type PopenT = sp.Popen[str] | sp.Popen[bytes]


# ---------------------------------------------------------------------------------------
# command line
# ---------------------------------------------------------------------------------------


@dataclass
class FlowArgs(argparse.Namespace):
    path_to_flow: str = ""
    config_file: str | None = None
    cmds: list[str] = field(default_factory=list)
    components: list[str] = field(default_factory=list)
    path_to_channel: str = DEFAULT_PATH_TO_CHANNEL
    host: str | None = None
    channel_host: str | None = None
    log_level: str = "INFO"
    component_log_level: str | None = None
    verbose_channels: bool = False


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value]


def load_toml_defaults(path: str) -> dict[str, Any]:
    """Load a --config TOML file, a flat table of the same options as the '--' flags below."""
    try:
        with Path(path).open("rb") as f:
            return tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        msg = f"Couldn't read --config TOML file {path}: {e}"
        raise RuntimeError(msg) from e


def apply_toml_defaults(namespace: FlowArgs, defaults: dict[str, Any]) -> None:
    """Seed namespace's fields from a --config TOML file before argparse runs on it.

    argparse only fills in an option's own `default=` when the namespace doesn't already have
    that attribute, and namespace here is a FlowArgs instance whose dataclass field defaults
    already give every field a value, so a plain `add_argument(default=...)` would never
    actually apply. Setting the field directly beforehand and only having argparse overwrite it
    when the option is *actually given* on the command line is what makes '--' flags override
    the TOML file rather than the other way around.

    path_to_flow is the one exception: as a positional with nargs="?", argparse *does* apply
    its own default even onto a pre-seeded namespace, so it's passed straight into
    create_args_parser(path_to_flow_default=...) instead of being handled here.
    """
    if "cmds" in defaults:
        namespace.cmds = _as_str_list(defaults["cmds"])
    if "components" in defaults:
        namespace.components = _as_str_list(defaults["components"])
    if "path_to_channel" in defaults:
        namespace.path_to_channel = str(defaults["path_to_channel"])
    if "host" in defaults:
        namespace.host = str(defaults["host"])
    if "channel_host" in defaults:
        namespace.channel_host = str(defaults["channel_host"])
    if "log_level" in defaults:
        namespace.log_level = str(defaults["log_level"])
    if "component_log_level" in defaults:
        namespace.component_log_level = str(defaults["component_log_level"])
    if "verbose_channels" in defaults:
        namespace.verbose_channels = bool(defaults["verbose_channels"])


def create_args_parser(*, path_to_flow_default: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an FBP flow (JSON) of standard and/or process style components.",
    )
    _ = parser.add_argument(
        "path_to_flow",
        type=str,
        # unlike the '--' options below, a positional's nargs="?" default *is* applied by
        # argparse even onto a pre-seeded namespace, so the TOML value has to be passed here
        # directly rather than via apply_toml_defaults
        nargs="?" if path_to_flow_default is not None else None,
        default=path_to_flow_default,
        help="Path to the flow JSON file to be run. May also be given as 'path_to_flow' in --config.",
    )
    _ = parser.add_argument(
        "--config",
        "-e",
        dest="config_file",
        type=str,
        default=None,
        help=(
            "Path to a TOML file with a flat table providing defaults for any of the other options here "
            "(and optionally 'path_to_flow'), so a whole environment (which channel binary, which cmds/"
            "components files, hosts, log levels, ...) can be captured in one file instead of repeating "
            "'--' flags. Explicit '--' flags on the command line still override the TOML file's values."
        ),
    )
    _ = parser.add_argument(
        "--cmds",
        "-c",
        type=str,
        nargs="+",
        action="extend",
        default=[],
        help=(
            "Path(s) to local_cmds.json file(s) mapping component ids to the command starting them. "
            "May be given multiple times; on duplicate component ids the last file wins. "
            "If omitted (here and in --config), the 'cmds' entry of the flow file is used."
        ),
    )
    _ = parser.add_argument(
        "--components",
        "-m",
        type=str,
        nargs="+",
        action="extend",
        default=[],
        help=(
            "Path(s) to component metadata cache JSON file(s) (as written by the local components service). "
            "Used to find out whether a component is of type 'standard' or 'process' without starting it. "
            "Metadata of components not found in a cache is queried by running '<cmd> -O'."
        ),
    )
    _ = parser.add_argument(
        "--path_to_channel",
        "-p",
        type=str,
        default=DEFAULT_PATH_TO_CHANNEL,
        help=f"Path to the channel executable. Default: {DEFAULT_PATH_TO_CHANNEL}",
    )
    _ = parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host/IP this starter serves its process capability writers on. Default: auto detected.",
    )
    _ = parser.add_argument(
        "--channel_host",
        type=str,
        default=None,
        help=(
            "Host/IP the started channels bind to. Only set this if the channels should not listen on all "
            "interfaces; the channels still advertise their outside IP in their sturdy refs, so binding them "
            "to e.g. 127.0.0.1 makes them unreachable."
        ),
    )
    _ = parser.add_argument(
        "--component_log_level",
        type=str,
        default=None,
        help="Log level passed on to the started components. Default: the starter's log level.",
    )
    _ = parser.add_argument(
        "--verbose_channels",
        action="store_true",
        help="Start the channels in verbose mode.",
    )
    add_log_level_argument(parser, default_level="INFO")
    return parser


# ---------------------------------------------------------------------------------------
# flow file parsing (tolerant against camelCase and snake_case flow versions)
# ---------------------------------------------------------------------------------------


def _first_of(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return default


@dataclass(frozen=True)
class PortRef:
    node_id: str
    port: str


@dataclass(frozen=True)
class FlowLink:
    src: PortRef
    tgt: PortRef

    @property
    def chan_id(self) -> str:
        """Base64 encoded json identifying this link, used as channel startup info id."""
        encoded = base64.urlsafe_b64encode(
            json.dumps(
                {
                    "out": {"nodeId": self.src.node_id, "port": self.src.port},
                    "in": {"nodeId": self.tgt.node_id, "port": self.tgt.port},
                },
            ).encode(),
        )
        return encoded.decode("ascii").rstrip("=")


def decode_chan_id(chan_id: str) -> FlowLink:
    missing_padding = len(chan_id) % 4
    padded = chan_id + ("=" * (4 - missing_padding) if missing_padding else "")
    decoded = json.loads(base64.urlsafe_b64decode(padded).decode())
    return FlowLink(
        src=PortRef(decoded["out"]["nodeId"], decoded["out"]["port"]),
        tgt=PortRef(decoded["in"]["nodeId"], decoded["in"]["port"]),
    )


@dataclass
class FlowNode:
    node_id: str
    name: str
    component_id: str | None = None
    cmd: str | None = None
    parallel_count: int = 1
    config: dict[str, Any] | None = None
    config_is_connected: bool = False
    content: Any = None
    metadata: ComponentMetadata | None = None
    kind: NodeKind = "standard"

    @property
    def is_iip(self) -> bool:
        return self.kind == "iip"


def _port_ref(d: dict[str, Any]) -> PortRef:
    return PortRef(str(_first_of(d, "nodeId", "node_id", default="")), str(_first_of(d, "port", default="")))


def parse_config(raw_config: Any) -> dict[str, Any] | None:
    """Flow configs are either a JSON object or a TOML/JSON string."""
    if raw_config is None:
        return None
    if isinstance(raw_config, dict):
        return dict(raw_config)
    if isinstance(raw_config, str):
        if not raw_config.strip():
            return None
        try:
            return dict(tomllib.loads(raw_config))
        except tomllib.TOMLDecodeError:
            pass
        try:
            loaded = json.loads(raw_config)
        except json.JSONDecodeError:
            logger.warning("Couldn't parse config as TOML or JSON, ignoring it:\n%s", raw_config)
            return None
        return dict(loaded) if isinstance(loaded, dict) else None
    logger.warning("Unsupported config type %s, ignoring it.", type(raw_config).__name__)
    return None


def parse_flow(flow_json: dict[str, Any]) -> tuple[dict[str, FlowNode], list[FlowLink]]:
    nodes: dict[str, FlowNode] = {}
    for node_json in flow_json.get("nodes", []):
        node_id = _first_of(node_json, "nodeId", "node_id")
        if node_id is None:
            logger.warning("Skipping node without node id: %s", node_json)
            continue
        node_id = str(node_id)
        component_id = _first_of(node_json, "componentId", "component_id")
        cmd = None
        if isinstance(component := node_json.get("component"), dict):
            cmd = component.get("cmd")
        nodes[node_id] = FlowNode(
            node_id=node_id,
            name=str(_first_of(node_json, "processName", "process_name", default=node_id)),
            component_id=str(component_id) if component_id is not None else None,
            cmd=str(cmd) if cmd else None,
            parallel_count=max(1, int(_first_of(node_json, "parallelProcesses", "parallel_processes", default=1))),
            config=parse_config(node_json.get("config")),
            content=node_json.get("content"),
            kind="iip" if "content" in node_json else "standard",
        )

    links: list[FlowLink] = []
    for link_json in flow_json.get("links", []):
        src_json = _first_of(link_json, "source", "out")
        tgt_json = _first_of(link_json, "target", "in")
        if not isinstance(src_json, dict) or not isinstance(tgt_json, dict):
            logger.warning("Skipping malformed link: %s", link_json)
            continue
        link = FlowLink(src=_port_ref(src_json), tgt=_port_ref(tgt_json))
        if link.src.node_id not in nodes or link.tgt.node_id not in nodes:
            logger.warning("Skipping link referencing unknown node(s): %s", link_json)
            continue
        links.append(link)

    return nodes, links


def load_cmds(paths: list[str]) -> dict[str, str]:
    """Merge the given local_cmds.json files, later files win on duplicate component ids."""
    component_id_to_cmd: dict[str, str] = {}
    for path in paths:
        try:
            with Path(path).open() as f:
                loaded = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.exception("Couldn't read cmds file %s", path)
            continue
        for component_id, cmd in loaded.items():
            # 'id' and 'name' describe the cmds file itself, '___' prefixed ids are disabled
            if component_id in ("id", "name") or component_id.startswith("___"):
                continue
            if isinstance(cmd, str):
                component_id_to_cmd[component_id] = cmd
    return component_id_to_cmd


def load_metadata_caches(paths: list[str]) -> dict[str, dict[str, Any]]:
    cache: dict[str, dict[str, Any]] = {}
    for path in paths:
        try:
            with Path(path).open() as f:
                loaded = json.load(f)
        except (OSError, json.JSONDecodeError):
            logger.exception("Couldn't read component metadata cache %s", path)
            continue
        for component_id, meta in loaded.items():
            if isinstance(meta, dict):
                cache[component_id] = meta
    return cache


def cmd_to_popen_args(cmd: str) -> list[str]:
    args = cmd.split(" ")
    if args and args[0] == "python":
        args[0] = sys.executable
    return args


def query_component_metadata(cmd: str) -> dict[str, Any] | None:
    """Run '<cmd> -O' to get the component's metadata as JSON."""
    args = [*cmd_to_popen_args(cmd), "-O"]
    try:
        res = sp.run(args, stdout=sp.PIPE, text=True, check=False)  # noqa: S603
        return json.loads(res.stdout)
    except (OSError, json.JSONDecodeError, sp.SubprocessError, ValueError):
        logger.warning("Couldn't retrieve component metadata via '%s'.", " ".join(args))
        return None


def resolve_metadata(cmd: str, component_id: str | None, cache: dict[str, dict[str, Any]]) -> ComponentMetadata | None:
    meta_json = cache.get(component_id) if component_id else None
    if meta_json is None:
        meta_json = query_component_metadata(cmd)
        if meta_json is not None and component_id:
            cache[component_id] = meta_json
    if meta_json is None:
        return None
    try:
        return ComponentMetadata.model_validate(meta_json)
    except (TypeError, ValidationError, ValueError):
        logger.warning("Invalid metadata for component id=%s.", component_id)
        return None


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:64]


def listening_port(server: Any) -> int:
    """Port of a server's IPv4 socket (or of its first socket if there is no IPv4 one)."""
    sockets = list(server.sockets)
    ipv4_sockets = [s for s in sockets if s.family == socket.AddressFamily.AF_INET]
    return (ipv4_sockets or sockets)[0].getsockname()[1]


def as_localhost_sturdy_ref(
    sturdy_ref: str | SturdyRefBuilder | SturdyRefReader,
) -> str | SturdyRefBuilder | None:
    """Copy of sturdy_ref with its host forced to 127.0.0.1, or None if it already points there.

    The channel binary advertises an auto-detected local network address in its sturdy refs,
    overriding any explicit --local_host, and that address can be unreachable on machines which
    restrict listening on non-loopback interfaces. Since every sturdy ref this starter connects
    to belongs to a channel or component it just started as a local subprocess, retrying via
    127.0.0.1 is always a valid fallback here.
    """
    if isinstance(sturdy_ref, str):
        parsed = urlp.urlparse(sturdy_ref)
        if parsed.hostname in LOOPBACK_HOSTS:
            return None
        userinfo = f"{parsed.username}@" if parsed.username else ""
        port = f":{parsed.port}" if parsed.port else ""
        return parsed._replace(netloc=f"{userinfo}127.0.0.1{port}").geturl()

    if str(sturdy_ref.vat.address.host) in LOOPBACK_HOSTS:
        return None
    # readers expose as_builder() directly; builders don't (they already are one), so round-trip
    # through as_reader() first to get an independent copy and leave the original untouched.
    local_ref = sturdy_ref.as_reader().as_builder() if hasattr(sturdy_ref, "as_reader") else sturdy_ref.as_builder()
    local_ref.vat.address.host = "127.0.0.1"
    return local_ref


def local_sr(sturdy_ref: str | SturdyRefBuilder | SturdyRefReader) -> str | SturdyRefBuilder:
    """Owned copy of sturdy_ref with its host forced to 127.0.0.1.

    Unlike as_localhost_sturdy_ref, this is meant to be applied proactively to every sturdy ref
    produced by a locally started channel - not just as a reconnect fallback - because most of
    these sturdy refs are handed off unmodified to other locally started subprocesses (other
    channels' --startup_info_writer_sr, standard/process components' port sturdy refs), which
    connect to them on their own and never go through this starter's connect_or_raise. It always
    returns an independent copy (even if already loopback), since struct inputs are typically
    readers into a short lived message that shouldn't be held onto directly.
    """
    if isinstance(sturdy_ref, str):
        return as_localhost_sturdy_ref(sturdy_ref) or sturdy_ref
    local_ref = sturdy_ref.as_reader().as_builder() if hasattr(sturdy_ref, "as_reader") else sturdy_ref.as_builder()
    if str(local_ref.vat.address.host) not in LOOPBACK_HOSTS:
        local_ref.vat.address.host = "127.0.0.1"
    return local_ref


def structured_text_ip(config: dict[str, Any]):
    return fbp_capnp.IP.new_message(
        content=common_capnp.StructuredText.new_message(type="json", value=json.dumps(config)),
    )


# ---------------------------------------------------------------------------------------
# capnp servers used to talk to process style components
# ---------------------------------------------------------------------------------------


class ProcessCapWriter(fbp_capnp.Channel.Writer.Server):
    """A one shot writer a started process component writes its Process capability into."""

    def __init__(self, name: str):
        self.name: str = name
        self.process_cap_received: asyncio.Future[ProcessClient] = asyncio.Future()

    @override
    async def write_context(self, context):
        if context.params.which() == "value" and not self.process_cap_received.done():
            self.process_cap_received.set_result(context.params.value.as_interface(fbp_capnp.Process))


class ProcessFinishedWatcher(fbp_capnp.Process.StateTransition.Server):
    """Sets an event as soon as a process' run method returned (or failed)."""

    def __init__(self, name: str):
        self.name: str = name
        self.finished: asyncio.Event = asyncio.Event()
        self.failed: bool = False

    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        logger.info("%s: state %s -> %s", self.name, old, new)
        if new in ("idle", "failed"):
            self.failed = new == "failed"
            self.finished.set()


@dataclass
class ConfigChannel:
    """Reader/writer of one of the config channels used to configure a standard component."""

    reader_sr: str
    writer_sr: SturdyRefBuilder


@dataclass
class StartedProcess:
    node_id: str
    name: str
    proc: PopenT
    cap: ProcessClient
    watcher: ProcessFinishedWatcher


# ---------------------------------------------------------------------------------------
# the flow runner
# ---------------------------------------------------------------------------------------


class FlowRunner:
    def __init__(self, args: FlowArgs):
        self.args: FlowArgs = args
        self.component_log_level: str = args.component_log_level or args.log_level
        self.restorer: common.Restorer = common.Restorer()
        self.con_man: common.ConnectionManager = common.ConnectionManager(self.restorer)

        self.nodes: dict[str, FlowNode] = {}
        self.links: list[FlowLink] = []

        # node_id -> port name -> list of channel sturdy refs (list because of array ports)
        self.in_srs: defaultdict[str, defaultdict[str, list[SturdyRefBuilder]]] = defaultdict(
            lambda: defaultdict(list),
        )
        self.out_srs: defaultdict[str, defaultdict[str, list[SturdyRefBuilder]]] = defaultdict(
            lambda: defaultdict(list),
        )

        self.channels: list[PopenT] = []
        self.standard_procs: dict[str, list[PopenT]] = {}
        self.started_processes: list[StartedProcess] = []
        self.port_infos_writers: list[WriterClient] = []
        self.sink_node_ids: list[str] = []

    # -- setup ---------------------------------------------------------------------------

    def load_flow(self) -> None:
        with Path(self.args.path_to_flow).open() as f:
            flow_json = json.load(f)
        if not flow_json:
            msg = f"Couldn't read flow file {self.args.path_to_flow}."
            raise RuntimeError(msg)

        self.nodes, self.links = parse_flow(flow_json)

        cmds_paths = self.args.cmds
        if not cmds_paths:
            flow_cmds = flow_json.get("cmds")
            cmds_paths = [flow_cmds] if isinstance(flow_cmds, str) else list(flow_cmds or [])
            if cmds_paths:
                logger.info("No --cmds given, using 'cmds' entry of the flow file: %s", cmds_paths)
        component_id_to_cmd = load_cmds(cmds_paths)
        metadata_cache = load_metadata_caches(self.args.components)

        self._resolve_nodes(component_id_to_cmd, metadata_cache)
        self._mark_connected_config_ports()
        self._generate_config_iips()

    def _resolve_nodes(self, component_id_to_cmd: dict[str, str], metadata_cache: dict[str, dict[str, Any]]) -> None:
        for node in self.nodes.values():
            if node.is_iip or node.component_id == IIP_COMPONENT_ID:
                node.kind = "iip"
                continue
            if node.cmd is None and node.component_id:
                node.cmd = component_id_to_cmd.get(node.component_id)
            if node.cmd is None:
                msg = (
                    f"No command found for node '{node.name}' (component id={node.component_id}). "
                    "Add it to a local_cmds.json passed via --cmds."
                )
                raise RuntimeError(msg)

            node.metadata = resolve_metadata(node.cmd, node.component_id, metadata_cache)
            if node.metadata is None:
                logger.warning(
                    "Couldn't determine metadata of node '%s' (component id=%s), assuming a 'standard' component.",
                    node.name,
                    node.component_id,
                )
                node.kind = "standard"
            else:
                node.kind = "process" if node.metadata.type == "process" else "standard"

    def _mark_connected_config_ports(self) -> None:
        for link in self.links:
            if link.tgt.port == "conf" and (node := self.nodes.get(link.tgt.node_id)) is not None:
                node.config_is_connected = True

    def _generate_config_iips(self) -> None:
        """Standard components can only be configured via an IIP on their 'conf' port.

        Process components get their config via setConfigEntry, so nothing is generated for them.
        """
        for node in list(self.nodes.values()):
            if node.kind != "standard" or not node.config or node.config_is_connected:
                continue
            iip_id = str(uuid.uuid4())
            self.nodes[iip_id] = FlowNode(
                node_id=iip_id,
                name=f"config of {node.name}",
                component_id=IIP_COMPONENT_ID,
                content=node.config,
                kind="iip",
            )
            self.links.append(FlowLink(src=PortRef(iip_id, "Right"), tgt=PortRef(node.node_id, "conf")))

    @property
    def standard_nodes(self) -> list[FlowNode]:
        return [n for n in self.nodes.values() if n.kind == "standard"]

    @property
    def process_nodes(self) -> list[FlowNode]:
        return [n for n in self.nodes.values() if n.kind == "process"]

    # -- channels ------------------------------------------------------------------------

    async def start_channels(self) -> tuple[ReaderClient, str, str | None]:
        """Start the startup info channel plus one channel per link and one config channel service."""
        first_chan, first_reader_sr, first_writer_sr = chans.start_first_channel(
            self.args.path_to_channel,
            name="startup_info",
            host=self.args.channel_host,
        )
        self.channels.append(first_chan)
        if not first_reader_sr or not first_writer_sr:
            msg = "Couldn't get the sturdy refs of the startup info channel."
            raise RuntimeError(msg)
        # first_writer_sr in particular is handed to every other locally started channel via
        # --startup_info_writer_sr, so it must already be reachable, not just retried on failure.
        first_reader_sr = local_sr(first_reader_sr)
        first_writer_sr = local_sr(first_writer_sr)

        first_reader_cap = await self.connect_or_raise(first_reader_sr, "startup info reader")
        first_reader = first_reader_cap.cast_as(fbp_capnp.Channel.Reader)

        for link in self.links:
            self.channels.append(
                chans.start_channel(
                    self.args.path_to_channel,
                    link.chan_id,
                    first_writer_sr,
                    name=sanitize_name(f"{link.src.node_id}.{link.src.port}-{link.tgt.node_id}.{link.tgt.port}"),
                    verbose=self.args.verbose_channels,
                    host=self.args.channel_host,
                ),
            )

        config_chan_id: str | None = None
        no_of_standard_components = len(self.standard_nodes)
        if no_of_standard_components > 0:
            config_chan_id = str(uuid.uuid4())
            self.channels.append(
                chans.start_channel(
                    self.args.path_to_channel,
                    config_chan_id,
                    first_writer_sr,
                    no_of_channels=no_of_standard_components,
                    name="port_infos",
                    verbose=self.args.verbose_channels,
                    host=self.args.channel_host,
                ),
            )

        return first_reader, first_writer_sr, config_chan_id

    async def collect_channel_srs(
        self,
        first_reader: ReaderClient,
        config_chan_id: str | None,
    ) -> list[ConfigChannel]:
        """Read all channel startup infos, send IIPs and remember the port sturdy refs."""
        config_chans: list[ConfigChannel] = []
        expected = len(self.links) + len(self.standard_nodes)
        while expected > 0:
            pair = (await first_reader.read()).value.as_struct(common_capnp.Pair)
            chan_id = pair.fst.as_text()
            info = pair.snd.as_struct(fbp_capnp.Channel.StartupInfo)
            expected -= 1

            if config_chan_id is not None and chan_id == config_chan_id:
                config_chans.append(
                    ConfigChannel(
                        # the reader sr goes on the component's command line, so it has to be a string
                        reader_sr=local_sr(common.sturdy_ref_str_from_sr(info.readerSRs[0])),
                        writer_sr=local_sr(info.writerSRs[0]),
                    ),
                )
                continue

            link = decode_chan_id(chan_id)
            src_node = self.nodes.get(link.src.node_id)
            if src_node is not None and src_node.is_iip:
                await self.send_iip(src_node, local_sr(info.writerSRs[0]))
            else:
                self.out_srs[link.src.node_id][link.src.port].append(local_sr(info.writerSRs[0]))
            self.in_srs[link.tgt.node_id][link.tgt.port].append(local_sr(info.readerSRs[0]))

        return config_chans

    async def send_iip(self, node: FlowNode, writer_sr: SturdyRefBuilder | SturdyRefReader) -> None:
        writer_cap = await self.connect_or_raise(writer_sr, f"IIP writer of '{node.name}'")
        writer = writer_cap.cast_as(fbp_capnp.Channel.Writer)
        content = node.content
        out_ip = structured_text_ip(content) if isinstance(content, dict) else fbp_capnp.IP.new_message(content=content)
        await writer.write(value=out_ip)
        await writer.write(done=None)
        await writer.close()
        logger.info("%s: sent IIP", node.name)

    # -- starting components -------------------------------------------------------------

    def port_infos(self, node: FlowNode) -> dict[str, list[dict[str, Any]]]:
        """Collect the connected ports of a node the way old style components expect them."""
        array_out_ports = (
            {p.name for p in node.metadata.outPorts if p.type == "array"} if node.metadata is not None else set()
        )
        in_ports = [{"name": name, "sr": srs[0]} for name, srs in self.in_srs[node.node_id].items() if srs]
        out_ports: list[dict[str, Any]] = []
        for name, srs in self.out_srs[node.node_id].items():
            if not srs:
                continue
            if name in array_out_ports or len(srs) > 1:
                out_ports.append({"name": name, "srs": list(srs)})
            else:
                out_ports.append({"name": name, "sr": srs[0]})
        return {"inPorts": in_ports, "outPorts": out_ports}

    def port_infos_message(self, node: FlowNode):
        """Build the PortInfos message the old style standard components expect."""
        ports = self.port_infos(node)
        port_infos = fbp_capnp.PortInfos.new_message()
        port_infos.inPorts = ports["inPorts"]
        port_infos.outPorts = ports["outPorts"]
        return port_infos

    async def start_standard_components(self, config_chans: list[ConfigChannel]) -> None:
        for node in self.standard_nodes:
            if not self.out_srs[node.node_id]:
                self.sink_node_ids.append(node.node_id)
            port_infos = self.port_infos_message(node)
            config_srs = config_chans.pop()
            procs: list[PopenT] = []
            for i in range(node.parallel_count):
                name = node.name if node.parallel_count == 1 else f"{node.name} {i + 1}"
                procs.append(
                    comp.start_local_component(
                        node.cmd or "",
                        config_srs.reader_sr,
                        name=name,
                        log_level=self.component_log_level,
                    ),
                )
                logger.info("%s: started standard component", name)
            self.standard_procs[node.node_id] = procs

            # send the port infos once per config channel; all parallel instances read from
            # the same channel, so write as many messages as there are instances
            writer_cap = await self.connect_or_raise(
                config_srs.writer_sr,
                f"port infos writer of '{node.name}'",
            )
            writer = writer_cap.cast_as(fbp_capnp.Channel.Writer)
            for _ in range(node.parallel_count):
                await writer.write(value=port_infos)
            # don't close the writer, it has to stay alive to forward cap calls and is used
            # as the signal letting the component shut down
            self.port_infos_writers.append(writer)

    async def start_process_components(self) -> None:
        for node in self.process_nodes:
            for i in range(node.parallel_count):
                name = node.name if node.parallel_count == 1 else f"{node.name} {i + 1}"
                await self.start_process_component(node, name)

    async def start_process_component(self, node: FlowNode, name: str) -> None:
        writer = ProcessCapWriter(name)
        save_sr_token, _unsave_sr_token = await self.restorer.save_cap(writer)
        writer_sr = self.restorer.sturdy_ref_str(save_sr_token)

        process_proc = proc.start_local_process_component(
            node.cmd or "",
            writer_sr,
            name=name,
            log_level=self.component_log_level,
        )
        logger.info("%s: started process component", name)

        try:
            async with asyncio.timeout(PROCESS_CAP_TIMEOUT_SECONDS):
                process_cap = await writer.process_cap_received
        except TimeoutError as e:
            process_proc.terminate()
            msg = f"'{name}' didn't send its process capability within {PROCESS_CAP_TIMEOUT_SECONDS}s."
            raise RuntimeError(msg) from e

        watcher = ProcessFinishedWatcher(name)
        _ = await process_cap.state(transitionCallback=watcher)

        for port_name, srs in self.in_srs[node.node_id].items():
            for sr in srs:
                connected = (await process_cap.connectInPort(port_name, sr)).connected
                if not connected:
                    logger.warning("%s: couldn't connect in port '%s'", name, port_name)
        for port_name, srs in self.out_srs[node.node_id].items():
            for sr in srs:
                connected = (await process_cap.connectOutPort(port_name, sr)).connected
                if not connected:
                    logger.warning("%s: couldn't connect out port '%s'", name, port_name)

        if node.config and not node.config_is_connected:
            await self.apply_config(process_cap, node, name)

        if not await process_cap.start():
            logger.warning("%s: process didn't start", name)

        self.started_processes.append(
            StartedProcess(node_id=node.node_id, name=name, proc=process_proc, cap=process_cap, watcher=watcher),
        )

    @staticmethod
    async def apply_config(process_cap: ProcessClient, node: FlowNode, name: str) -> None:
        for key, value in (node.config or {}).items():
            # a None value means 'use the component's default', which is what happens anyway
            # if the entry is not set at all
            if value is None:
                continue
            try:
                # setConfigEntry has a named param struct, so it can neither be called with
                # positional args nor with a 'name' keyword (that one belongs to _send)
                request = process_cap.setConfigEntry_request()
                request.name = key
                request.val = config_value_from_python(value)
                _ = await request.send()
            except (capnp.KjException, TypeError):
                logger.exception("%s: couldn't set config entry '%s'", name, key)

    # -- running and tear down -----------------------------------------------------------

    async def wait_for_flow(self) -> None:
        """Wait until every process component's run returned and every standard sink exited."""
        waiters: list[asyncio.Task[None]] = [
            asyncio.create_task(self.wait_for_process_component(sp_), name=sp_.name) for sp_ in self.started_processes
        ]
        waiters.extend(
            asyncio.create_task(self.wait_for_standard_component(node_id, p), name=f"{node_id} exit")
            for node_id in self.sink_node_ids
            for p in self.standard_procs.get(node_id, [])
        )
        if waiters:
            _ = await asyncio.gather(*waiters)

        failed = [sp_.name for sp_ in self.started_processes if sp_.watcher.failed]
        if failed:
            logger.error("these process components failed: %s", ", ".join(failed))
        else:
            logger.info("all components finished")

    async def wait_for_process_component(self, started: StartedProcess) -> None:
        """A process component is done when its run returned - or when it died unexpectedly."""
        finished = asyncio.create_task(started.watcher.finished.wait())
        exited = asyncio.create_task(self.wait_for_exit(started.proc))
        _done, pending = await asyncio.wait({finished, exited}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            _ = task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if not started.watcher.finished.is_set():
            started.watcher.failed = True
            logger.error("%s: process exited before its run finished", started.name)

    async def wait_for_standard_component(self, node_id: str, process: PopenT) -> None:
        _ = await self.wait_for_exit(process)
        logger.info("%s: standard component exited", self.nodes[node_id].name)

    @staticmethod
    async def wait_for_exit(process: PopenT, timeout_seconds: float | None = None) -> bool:
        try:
            async with asyncio.timeout(timeout_seconds):
                while process.poll() is None:
                    await asyncio.sleep(PROCESS_EXIT_POLL_SECONDS)
        except TimeoutError:
            return False
        return True

    async def shutdown(self) -> None:
        for started in self.started_processes:
            with contextlib.suppress(capnp.KjException, RuntimeError, TimeoutError):
                async with asyncio.timeout(PROCESS_STOP_TIMEOUT_SECONDS):
                    _ = await started.cap.stop()
        for started in self.started_processes:
            if started.proc.poll() is None:
                started.proc.terminate()
                if not await self.wait_for_exit(started.proc, PROCESS_STOP_TIMEOUT_SECONDS):
                    started.proc.kill()
        self.started_processes.clear()

        for procs in self.standard_procs.values():
            for process in procs:
                if process.poll() is None:
                    process.terminate()
        self.standard_procs.clear()

        self.port_infos_writers.clear()

        for channel in self.channels:
            with contextlib.suppress(OSError):
                channel.terminate()
        self.channels.clear()
        logger.info("all channels terminated")

    async def connect_or_raise(self, sturdy_ref: str | SturdyRefBuilder | SturdyRefReader, target: str):
        cap = await self.con_man.try_connect(sturdy_ref)
        if cap is None and (local_ref := as_localhost_sturdy_ref(sturdy_ref)) is not None:
            logger.warning(
                "Couldn't connect to %s at %s, retrying via 127.0.0.1 "
                "(channels/components started by this flow runner are always local)",
                target,
                sturdy_ref,
            )
            cap = await self.con_man.try_connect(local_ref)
        if cap is None:
            msg = f"Couldn't connect to {target} at {sturdy_ref}."
            raise RuntimeError(msg)
        return cap

    # -- entry point ---------------------------------------------------------------------

    async def run(self) -> None:
        self.load_flow()
        logger.info(
            "flow '%s': %d standard component(s), %d process component(s), %d link(s)",
            Path(self.args.path_to_flow).name,
            len(self.standard_nodes),
            len(self.process_nodes),
            len(self.links),
        )

        async def new_connection(stream: capnp.AsyncIoStream):
            await capnp.TwoPartyServer(stream, bootstrap=self.restorer).on_disconnect()

        # bind to a concrete host, else create_server opens an IPv4 and an IPv6 socket which get
        # different ephemeral ports, and the components would be sent the wrong one
        host = self.args.host or common.get_public_ip()
        server = await capnp.AsyncIoStream.create_server(new_connection, host, 0)
        self.restorer.host = host
        self.restorer.port = listening_port(server)
        logger.info("flow starter listening on %s:%s", self.restorer.host, self.restorer.port)

        # the components connect back to this server, so keep it up until everything is down
        async with server:
            try:
                first_reader, _first_writer_sr, config_chan_id = await self.start_channels()
                config_chans = await self.collect_channel_srs(first_reader, config_chan_id)
                await self.start_standard_components(config_chans)
                await self.start_process_components()
                await self.wait_for_flow()
            except Exception:
                logger.exception("exception terminated %s early", Path(__file__).name)
            finally:
                await self.shutdown()


def _find_config_file(argv: list[str] | None) -> str | None:
    """First pass: only look for --config/-e, so its values can seed the real parser's defaults."""
    pre_parser = argparse.ArgumentParser(add_help=False)
    _ = pre_parser.add_argument("--config", "-e", dest="config_file", type=str, default=None)
    pre_args, _unknown = pre_parser.parse_known_args(argv)
    return pre_args.config_file


async def start_flow(argv: list[str] | None = None) -> None:
    defaults = load_toml_defaults(config_file) if (config_file := _find_config_file(argv)) else {}
    namespace = FlowArgs()
    apply_toml_defaults(namespace, defaults)
    toml_flow = defaults.get("path_to_flow")
    parser = create_args_parser(path_to_flow_default=str(toml_flow) if toml_flow is not None else None)
    parser.parse_args(argv, namespace=namespace)
    args = namespace
    configure_logging(args.log_level)
    await FlowRunner(args).run()


def main() -> None:
    asyncio.run(capnp.run(start_flow()))


if __name__ == "__main__":
    main()
