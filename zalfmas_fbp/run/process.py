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
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess as sp
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, StateTransitionClient, WriterClient
    from mas.schema.fbp.fbp_capnp.types.enums import ProcessStateEnum
from zalfmas_common import common

ArrayWriterPorts = list["WriterClient | None"]

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ProcessStateEnum, ProcessStateEnum], None
        ],  #: Callable[[fbp_capnp.Process.State, fbp_capnp.Process.State]]
    ):
        self.callback: Callable[[ProcessStateEnum, ProcessStateEnum], None] = callback

    # stateChanged @0 (old :State, new :State);
    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class Process(fbp_capnp.Process.Server, common.Identifiable, common.GatewayRegistrable):
    def __init__(
        self,
        metadata: dict[str, Any] | None = None,
        con_man: common.ConnectionManager | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        common.GatewayRegistrable.__init__(self, con_man if con_man else common.ConnectionManager())

        self.metadata: dict[str, Any] = metadata if metadata else {}
        self.configuration: dict[str, Any] = {}
        self.in_ports: dict[str, ReaderClient | None] = {}
        self.out_ports: dict[str, WriterClient | None] = {}
        self.array_out_ports: dict[str, ArrayWriterPorts] = {}
        self.tasks = []
        self.process_state: ProcessStateEnum = "stopped"  # states: started, stopped, canceled
        self.state_transition_callbacks: list[StateTransitionClient] = []

        self.init_from_metadata()

    @staticmethod
    def _is_array_port(port_info: dict[str, Any]) -> bool:
        return port_info.get("type") == "array"

    @staticmethod
    def _port_message(name: str, port_info: dict[str, Any] | None, port_type: str):
        port_info = port_info or {}
        return {
            "name": name,
            "type": port_type,
            "contentType": port_info.get("contentType", "Text"),
        }

    def init_from_metadata(self):
        default_config: dict[str, Any] = {}
        if self.meta:
            try:
                component_meta = self.meta["component"]
                default_config = {k: v["value"] for k, v in component_meta.get("defaultConfig", {}).items()}
                self.name: str = component_meta["info"]["name"]
                self.description: str = component_meta["info"]["description"]
                for port_info in component_meta.get("inPorts", []):
                    _ = self.in_ports.setdefault(port_info["name"], None)
                for port_info in component_meta.get("outPorts", []):
                    name = port_info["name"]
                    if self._is_array_port(port_info):
                        self.array_out_ports.setdefault(name, [])
                    else:
                        self.out_ports.setdefault(name, None)
            except Exception as e:
                logger.warning(
                    f"Some metadata could not be used for initializing the process component. Exception: {e}"
                )
        for k, v in default_config.items():
            val = None
            vt = type(v)
            if vt is str:
                val = common_capnp.Value.new_message(t=",")
            elif vt is int:
                val = common_capnp.Value.new_message(i64=v)
            elif vt is float:
                val = common_capnp.Value.new_message(f64=v)
            elif vt is bool:
                val = common_capnp.Value.new_message(b=v)
            elif vt is dict:
                val = common_capnp.Value.new_message(t=json.dumps(v))
            elif vt is list and len(vt) > 0 and type(vt[0]) is int:
                if all(map(lambda x: type(x) is int, v)):
                    val = common_capnp.Value.new_message(li64=v)
            elif vt is list and len(vt) > 0 and type(vt[0]) is float:
                if all(map(lambda x: type(x) is float, v)):
                    val = common_capnp.Value.new_message(lf64=v)
            elif vt is list and len(vt) > 0 and type(vt[0]) is bool:
                if all(map(lambda x: type(x) is bool, v)):
                    val = common_capnp.Value.new_message(lb=v)
            elif vt is list and len(vt) > 0 and type(vt[0]) is str:
                if all(map(lambda x: type(x) is str, v)):
                    val = common_capnp.Value.new_message(lt=v)

            if val:
                self.config[k] = val

    def is_canceled(self):
        return self.process_state == "canceled"

    @property
    def meta(self):
        return self.metadata

    @property
    def config(self):
        return self.configuration

    # inPorts @0 () -> (ports :List(Component.Port));
    async def inPorts(self, _context, **kwargs):
        component_meta = self.meta.get("component", {})
        in_port_infos = {p["name"]: p for p in component_meta.get("inPorts", [])}
        return [self._port_message(k, in_port_infos.get(k), "standard") for k in self.in_ports]

    # connectInPort @1 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectInPort(self, name: str, sturdyRef, _context, **kwargs):
        reader = (
            reader_cap.cast_as(fbp_capnp.Channel.Reader)
            if (reader_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        self.in_ports[name] = reader
        return self.in_ports[name] is not None

    # outPorts @2 () -> (ports :List(Component.Port));
    async def outPorts(self, _context, **kwargs):
        component_meta = self.meta.get("component", {})
        out_port_infos = {p["name"]: p for p in component_meta.get("outPorts", [])}
        ports = [self._port_message(k, out_port_infos.get(k), "standard") for k in self.out_ports]
        ports.extend(self._port_message(k, out_port_infos.get(k), "array") for k in self.array_out_ports)
        return ports

    # connectOutPort @3 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectOutPort(self, name: str, sturdyRef, _context, **kwargs):
        writer = (
            writer_cap.cast_as(fbp_capnp.Channel.Writer)
            if (writer_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        if name in self.array_out_ports:
            self.array_out_ports[name].append(writer)
            return writer is not None

        self.out_ports[name] = writer
        return self.out_ports[name] is not None

    # configEntries @4 () -> (config :List(ConfigEntry));
    async def configEntries(self, _context, **kwargs):
        return list(
            map(
                lambda k, v: fbp_capnp.Process.ConfigEntry.new_message(name=k, val=v),
                self.config.items(),
            )
        )

    # struct ConfigEntry {
    #     name @0 :Text;
    #     val  @1 :Common.Value;
    # }
    # setConfigEntry @7 ConfigEntry;
    async def setConfigEntry_context(self, context):
        ps = context.params
        self.config[ps.name] = ps.val.as_builder()
        # print(f"received config entry: {ps.name} with value: {ps.val}")

    async def process_started(self):
        await self.transition_to_state("started")

    async def process_stopped(self):
        await self.transition_to_state("stopped")

    async def transition_to_state(self, new_state: ProcessStateEnum):
        if new_state == self.process_state:
            return

        prev_state = self.process_state
        self.process_state = new_state
        for cb in self.state_transition_callbacks:
            await cb.stateChanged(prev_state, self.process_state)

    # start @5 () -> (started: Bool, finishedSuccessfully :Bool);
    @override
    async def start(self, _context, **kwargs):
        # only call run, if run has finished already
        if self.process_state == "started" or self.process_state == "canceled":
            return
        await self.transition_to_state("started")
        await self.run()

    # stop @6 ();
    @override
    async def stop(self, _context, **kwargs):
        await self.close_out_ports()
        await self.transition_to_state("canceled")

    async def run(self):
        logger.warning("run method unimplemented")

    # state @8 (transitionCallback :StateTransition) -> (currentState :State);
    @override
    async def state(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.state_transition_callbacks.append(transitionCallback)
        return self.process_state

    async def close_out_ports(self):
        for name, port in self.out_ports.items():
            if port is not None:
                try:
                    await port.close()
                    logger.info(f"closed out port '{name}'")
                except Exception as e:
                    logger.error(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")
        for name, ports in self.array_out_ports.items():
            for i, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        logger.info(f"closed array out port '{name}[{i}]'")
                    except Exception as e:
                        logger.error(f"Exception closing array out port '{name}[{i}]': {e}")

    async def serve(
        self,
        writer_sr: str | None = None,
        serve_bootstrap: bool = False,
        host: str | None = None,
        port: int | None = None,
    ):
        if writer_sr and len(writer_sr) > 0 and (writer_cap := await self.con_man.try_connect(writer_sr)) is not None:
            writer = writer_cap.cast_as(fbp_capnp.Channel.Writer)
            await writer.write(value=self)
            logging.info(f"wrote process cap into {writer_sr}")

        async def new_connection(stream: capnp.AsyncIoStream):
            await capnp.TwoPartyServer(stream, bootstrap=self if serve_bootstrap else None).on_disconnect()

        port = port if port else 0
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        async with server:
            if serve_bootstrap:
                host = host if host else common.get_public_ip()
                import socket

                ipv4_sockets = list(filter(lambda s: s.family == socket.AddressFamily.AF_INET, server.sockets))
                if len(ip4_socks := ipv4_sockets) > 0:
                    port = ip4_socks[0].getsockname()[1]
                print(f"Process({self.name}) SR: capnp://{host}:{port}")
            await server.serve_forever()


def start_local_process_component(path_to_executable, process_cap_writer_sr, name: str | None = None) -> sp.Popen[str]:
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    proc = sp.Popen(
        pte_split + [process_cap_writer_sr],
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc


def create_default_args_parser(
    component_description: str,
):
    parser = argparse.ArgumentParser(description=component_description)
    _ = parser.add_argument(
        "process_cap_writer_sr",
        type=str,
        nargs="?",
        help="SturdyRef to the Writer[fbp.capnp:Process]. Writes process capability on startup to writer.",
    )
    _ = parser.add_argument(
        "--output_json_default_config",
        "-o",
        action="store_true",
        help="Output JSON configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--output_json_component_metadata",
        "-O",
        action="store_true",
        help="Output JSON component metadata at commandline. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "--write_json_default_config",
        "-w",
        type=str,
        help="Output JSON configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--write_json_component_metadata",
        "-W",
        type=str,
        help="Output JSON component metadata in the current directory. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "-b",
        "--serve_bootstrap",
        action="store_true",
        help="Serve process as the bootstrap object.>",
    )
    _ = parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to be used when serving the process.",
    )
    _ = parser.add_argument(
        "-l",
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="WARNING",
        help="Set logging level.",
    )
    return parser


def run_process_from_metadata_and_cmd_args(p: Process, component_meta):
    parser = create_default_args_parser(component_description=p.description)
    args = parser.parse_args()
    if component_meta:
        default_config = {k: v["value"] for k, v in component_meta["component"]["defaultConfig"].items()}
    else:
        default_config = {}
    if args.output_json_default_config:
        print(json.dumps(default_config, indent=4))
        exit(0)
    elif args.write_json_default_config:
        with open(args.write_json_default_config, "w") as _:
            json.dump(default_config, _, indent=4)
            exit(0)
    elif args.output_json_component_metadata:
        print(json.dumps(component_meta, indent=4))
        exit(0)
    elif args.write_json_component_metadata:
        with open(args.write_json_component_metadata, "w") as _:
            json.dump(component_meta, _, indent=4)
            exit(0)
    logger.setLevel(args.log_level)
    if args.process_cap_writer_sr:
        asyncio.run(
            capnp.run(
                p.serve(
                    writer_sr=args.process_cap_writer_sr,
                    serve_bootstrap=args.serve_bootstrap,
                    host=args.host,
                    port=args.port,
                )
            )
        )
    else:
        logger.error("A sturdy ref to a writer capability is necessary to start the process.")
