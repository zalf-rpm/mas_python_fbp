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
import logging
import os

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.persistence import persistence_capnp

# from zalfmas_capnp_schemas_with_stubs import common_capnp, fbp_capnp, persistence_capnp
from zalfmas_common import common

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback,  #: Callable[[fbp_capnp.Process.State, fbp_capnp.Process.State]]
    ):
        self.callback = callback

    # stateChanged @0 (old :State, new :State);
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class Process(fbp_capnp.Process.Server, common.Identifiable, common.GatewayRegistrable):
    def __init__(
        self,
        con_man: common.ConnectionManager = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        common.GatewayRegistrable.__init__(self, con_man if con_man else common.ConnectionManager())

        self.in_ports_config = {}
        self.out_ports_config = {}
        self.configuration = {}
        self.in_ports = {}
        self.out_ports = {}
        self.tasks = []
        self.process_state = "stopped"  # states: started, stopped, canceled
        self.state_transition_callbacks = []

    def is_canceled(self):
        return self.process_state == "canceled"

    @property
    def config(self):
        return self.configuration

    def ip(self, port_name: str):
        return self.in_ports.get(port_name, None)

    def close_ip(self, port_name: str):
        if port_name in self.in_ports:
            self.in_ports[port_name] = None

    def op(self, port_name: str):
        return self.out_ports.get(port_name, None)

    def close_op(self, port_name: str):
        if port_name in self.out_ports:
            self.out_ports[port_name] = None

    # inPorts @0 () -> (ports :List(Component.Port));
    async def inPorts(self, _context, **kwargs):
        return list(
            [
                {"name": k, "type": "standard", "contentType": "Text"}
                for k, v in self.in_ports.items()
            ]
        )

    # connectInPort @1 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectInPort(self, name: str, sturdyRef, _context, **kwargs):
        self.in_ports[name] = (await self.con_man.try_connect(sturdyRef)).cast_as(
            fbp_capnp.Channel.Reader
        )
        return self.in_ports[name] is not None

    # outPorts @2 () -> (ports :List(Component.Port));
    async def outPorts(self, _context, **kwargs):
        return list(
            [
                {"name": k, "type": "standard", "contentType": "Text"}
                for k, v in self.out_ports.items()
            ]
        )

    # connectOutPort @3 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectOutPort(self, name: str, sturdyRef, _context, **kwargs):
        self.out_ports[name] = (await self.con_man.try_connect(sturdyRef)).cast_as(
            fbp_capnp.Channel.Writer
        )
        return self.out_ports[name] is not None

    # configEntries @4 () -> (config :List(ConfigEntry));
    async def configEntries(self, _context, **kwargs):
        return list(
            map(
                lambda k, v: fbp_capnp.ConfigEntry.new_message(name=k, val=v),
                self.config.items(),
            )
        )

    # struct ConfigEntry {
    #     name @0 :Text;
    #     val  @1 :Common.Value;
    # }
    # setConfigEntry @7 ConfigEntry;
    async def setConfigEntry(self, name: str, value: common_capnp.ValueReader, _context, **kwargs):
        self.config[name] = value

    async def process_started(self):
        await self.transition_to_state("started")

    async def process_stopped(self):
        await self.transition_to_state("stopped")

    async def transition_to_state(self, new_state):
        if new_state == self.process_state:
            return

        prev_state = self.process_state
        self.process_state = new_state
        for cb in self.state_transition_callbacks:
            await cb.stateChanged(prev_state, self.process_state)

    # start @5 () -> (started: Bool, finishedSuccessfully :Bool);
    async def start(self, _context, **kwargs):
        await self.transition_to_state("started")
        await self.run()

    # stop @6 ();
    async def stop(self, _context, **kwargs):
        await self.close_out_ports()
        await self.transition_to_state("canceled")

    async def run(self):
        logger.warning("run method unimplemented")

    # state @8 (transitionCallback :StateTransition) -> (currentState :State);
    async def state(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.state_transition_callbacks.append(transitionCallback)
        return self.process_state

    async def close_out_ports(self):
        for name, ps in self.out_ports.items():
            # is an array out port
            if isinstance(ps, list):
                for i, p in enumerate(ps):
                    if p is not None:
                        try:
                            await p.close()
                            logger.info(f"closed array out port '{name}[{i}]'")
                        except Exception as e:
                            logger.error(f"Exception closing array out port '{name}[{i}]': {e}")
            # is a single out port
            elif ps is not None:
                try:
                    await ps.close()
                    logger.info(f"closed out port '{name}'")
                except Exception as e:
                    logger.error(
                        f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}"
                    )

    async def serve(
        self,
        writer_sr: str = None,
        serve_bootstrap: bool = False,
        host: str = None,
        port: int = None,
    ):
        if (
            writer_sr
            and len(writer_sr) > 0
            and (
                writer := (await self.con_man.try_connect(writer_sr)).cast_as(
                    fbp_capnp.Channel.Writer
                )
            )
        ):
            await writer.write(value=self)
            logging.info(f"wrote process cap into {writer_sr}")

        async def new_connection(stream):
            await capnp.TwoPartyServer(
                stream, bootstrap=self if serve_bootstrap else None
            ).on_disconnect()

        port = port if port else 0
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        async with server:
            if serve_bootstrap:
                host = host if host else common.get_public_ip()
                import socket

                if (
                    len(
                        ip4_socks := list(
                            filter(
                                lambda s: s.family == socket.AddressFamily.AF_INET,
                                server.sockets,
                            )
                        )
                    )
                    > 0
                ):
                    port = ip4_socks[0].getsockname()[1]
                print(f"Process({self.name}) SR: capnp://{host}:{port}")
            await server.serve_forever()


def parse_cmd_args_and_serve_process(p: Process):
    parser, args = create_default_args_parser(component_description=p.description)
    logger.setLevel(args.log_level)
    asyncio.run(
        capnp.run(
            p.serve(
                writer_sr=args.writer_sr,
                serve_bootstrap=args.serve_bootstrap,
                host=args.host,
                port=args.port,
            )
        )
    )


def create_default_args_parser(
    component_description: str,
):
    parser = argparse.ArgumentParser(description=component_description)
    parser.add_argument(
        "-w",
        "--writer_sr",
        type=str,
        default=None,
        help="SturdyRef to the Writer<fbp.capnp::Process>. Writes process capability on startup to writer.",
    )
    parser.add_argument(
        "-b",
        "--serve_bootstrap",
        action="store_true",
        help="Serve process as the bootstrap object.>",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to be used when serving the process.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to be used when serving the process.",
    )
    parser.add_argument(
        "-l",
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="WARNING",
        help="Set logging level.",
    )
    return parser, parser.parse_args()
