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
import argparse
import os

import capnp
from zalfmas_capnp_schemas_with_stubs import common_capnp, fbp_capnp
from zalfmas_common import common


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
        self._config = {}
        self._in_ports = {}
        self._out_ports = {}
        self.tasks = []

    @property
    def config(self):
        return self._config

    @property
    def in_ports(self):
        return self._in_ports

    @property
    def out_ports(self):
        return self._out_ports

    # inPorts @0 () -> (ports :List(Component.Port));
    async def inPorts(self):
        pass

    # connectInPort @1 (name :Text, sturdyRef :Text) -> (connected :Bool);
    async def connectInPort(self, name: str, sturdyRef: str):
        self.in_ports[name] = (await self.con_man.try_connect(sturdyRef)).cast_as(fbp_capnp.Channel.Reader)
        return self.in_ports[name] is not None

    # outPorts @2 () -> (ports :List(Component.Port));
    async def outPorts(self):
        pass

    # connectOutPort @3 (name :Text, sturdyRef :Text) -> (connected :Bool);
    async def connectOutPort(self, name: str, sturdyRef: str):
        self.out_ports[name] = (await self.con_man.try_connect(sturdyRef)).cast_as(fbp_capnp.Channel.Writer)
        return self.out_ports[name] is not None

    # configEntries @4 () -> (config :List(ConfigEntry));
    async def configEntries(self):
        return list(map(lambda k, v: fbp_capnp.ConfigEntry.new_message(name=k, val=v), self.config.items()))

    # setConfigValue @7 (name :Text, value :Common.Value);
    async def setConfigValue(self, name: str, value: common_capnp.Value):
        self.config[name] = value

    # start @5 ();
    async def start(self):
        pass

    # stop @6 ();
    async def stop(self):
        await self.close_out_ports()


    async def close_out_ports(
            self,
            print_info=False,
            print_exception=True,
    ):
        for name, ps in self.out_ports.items():
            # is an array out port
            if isinstance(ps, list):
                for i, p in enumerate(ps):
                    if p is not None:
                        try:
                            await p.close()
                            if print_info:
                                print(
                                    f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'"
                                )
                        except Exception as e:
                            if print_exception:
                                print(
                                    f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}"
                                )
            # is a single out port
            elif ps is not None:
                try:
                    await ps.close()
                    if print_info:
                        print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                except Exception as e:
                    if print_exception:
                        print(
                            f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}"
                        )

    async def serve(
        self,
        writer_sr: str,
        host: str = None,
        port: int = None,
        print_debug: bool = False,
    ):
        if writer := (await self.con_man.try_connect(writer_sr)).cast_as(
            fbp_capnp.Channel.Writer
        ):
            await writer.write(value=self)
            if print_debug:
                print(
                    f"{os.path.basename(__file__)}: wrote process cap into {writer_sr}"
                )

        async def new_connection(stream):
            await capnp.TwoPartyServer(stream, bootstrap=None).on_disconnect()

        port = port if port else 0
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        async with server:
            await server.serve_forever()


def create_default_args_parser(
        component_description: str,
):
    parser = argparse.ArgumentParser(description=component_description)
    parser.add_argument(
        "writer_sr",
        type=str,
        nargs="?",
        help="SturdyRef to Writer<Process.StartupInfo>",
    )
    return parser, parser.parse_args()
