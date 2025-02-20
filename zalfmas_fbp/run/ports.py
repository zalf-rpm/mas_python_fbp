#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import asyncio
import capnp
import os
import sys
import tomli
import uuid
from zalfmas_common import common
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

class PortConnector:

    def __init__(self, ins = None, outs = None, connection_manager=None):
        self.in_ports = {n: None for n in ins} if ins else {}
        self.out_ports = {n: None for n in outs} if outs else {}
        self.con_man = connection_manager if connection_manager else common.ConnectionManager()

    # make in_ports a property
    @property
    def ins(self):
        return self.in_ports

    # make out_ports a property
    @property
    def outs(self):
        return self.out_ports

    def __getitem__(self, key):
        if key in self.in_ports and key in self.out_ports:
            return {"in": self.in_ports[key], "out": self.out_ports[key]}
        if key in self.in_ports:
            return self.in_ports[key]
        if key in self.out_ports:
            return self.out_ports[key]
        return None

    def __setitem__(self, key, value):
        if (key in self.in_ports and key in self.out_ports and isinstance(value, dict) and
                "in" in value and "out" in value):
            self.in_ports[key] = value["in"]
            self.out_ports[key] = value["out"]
        if key in self.in_ports:
            self.in_ports[key] = value
        if key in self.out_ports:
            self.out_ports[key] = value

    async def close_out_ports(self, print_info=False, print_exception=True):
        for name, ps in self.out_ports.items():
            # is an array out port
            if isinstance(ps, list):
                for i, p in enumerate(ps):
                    if p is not None:
                        try:
                            await p.close()
                            if print_info:
                                print(f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'")
                        except Exception as e:
                            if print_exception:
                                print(f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}")
            # is a single out port
            elif ps is not None:
                try:
                    await ps.close()
                    if print_info:
                        print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                except Exception as e:
                    if print_exception:
                        print(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")

    @staticmethod
    async def create_from_cmd_config(config: dict, ins=None, outs=None, connection_manager=None):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_cmd_config(config)
        return pc

    async def connect_from_cmd_config(self, config: dict):
        try:
            for k, v in config.items():
                if k.endswith("in_sr"):
                    port_name = k[:-6]
                    if len(port_name) == 0:
                        port_name = "in"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.in_ports[port_name] = None
                    else:
                        self.in_ports[port_name] = await self.con_man.try_connect(v, cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
                elif k.endswith("out_sr"):
                    port_name = k[:-7]
                    if len(port_name) == 0:
                        port_name = "out"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.out_ports[port_name] = None
                    else:
                        self.in_ports[port_name] = await self.con_man.try_connect(v, cast_as=fbp_capnp.Channel.Writer, retry_secs=1)
                        self.out_ports[port_name] = self.in_ports[port_name]
                elif k.endswith("out_srs"):
                    port_name = k[:-8]
                    if len(port_name) == 0:
                        port_name = "out"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.out_ports[port_name] = None
                    else:
                        self.out_ports[port_name] = []
                        for out_sr in v.split("|"):
                            self.out_ports[port_name].append(await self.con_man.try_connect(out_sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                         retry_secs=1))
        except Exception as e:
            print(f"{os.path.basename(__file__)}: Exception connecting to ports via CMD config:\n{config}\n Exception: {e}")

    @staticmethod
    async def create_from_port_infos_reader(port_infos_reader_sr: str, ins=None, outs=None,
                                            connection_manager=None):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_port_infos_reader(port_infos_reader_sr)
        return pc

    async def connect_from_port_infos_reader(self, port_infos_reader_sr: str):
        try:
            pis_reader = await self.con_man.try_connect(port_infos_reader_sr,
                                                        cast_as=fbp_capnp.Channel.Reader,
                                                        retry_secs=1)
            pis = (await pis_reader.read()).value.as_struct(fbp_capnp.PortInfos)
            for n2sr in pis.inPorts:
                if len(n2sr.name) > 0:
                    port_name = n2sr.name
                    if n2sr.which() == "sr" and len(n2sr.sr) > 0:
                        self.in_ports[port_name] = await self.con_man.try_connect(n2sr.sr,
                                                                                   cast_as=fbp_capnp.Channel.Reader,
                                                                                   retry_secs=1)

            for n2sr in pis.outPorts:
                if len(n2sr.name) > 0:
                    port_name = n2sr.name
                    if n2sr.which() == "sr" and len(n2sr.sr) > 0:
                        self.out_ports[port_name] = await self.con_man.try_connect(n2sr.sr,
                                                                                    cast_as=fbp_capnp.Channel.Writer,
                                                                                    retry_secs=1)
                    elif len(n2sr.srs) > 0:
                        self.out_ports[port_name] = []
                        for sr in n2sr.srs:
                            if len(sr) > 0:
                                self.out_ports[port_name].append(await self.con_man.try_connect(sr,
                                                                                                 cast_as=fbp_capnp.Channel.Writer,
                                                                                                 retry_secs=1))

        except Exception as e:
            print(f"{os.path.basename(__file__)}: Exception connecting to ports via port infos reader SR:\n{port_infos_reader_sr}\n Exception: {e}")

    @staticmethod
    async def create_from_toml_str(config_toml_str: str, ins=None, outs=None, connection_manager=None):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_toml_str(config_toml_str)
        return pc

    async def connect_from_toml_str(self, config_toml_str: str):
        toml_config = tomli.loads(config_toml_str)

        try:
            for port_name, data in toml_config["ports"]["in"].items():
                sr = data.get("sr", None)
                sr = None if sr == "" else sr
                self.in_ports[port_name] = sr
                if sr:
                    self.in_ports[port_name] = await self.con_man.try_connect(sr, cast_as=fbp_capnp.Channel.Reader,
                                                                                  retry_secs=1)

            for port_name, data in toml_config["ports"]["out"].items():
                if isinstance(data, list):
                    self.out_ports[port_name] = []
                    for d in data:
                        sr = d.get("sr", None)
                        sr = None if sr == "" else sr
                        self.out_ports[port_name].append(sr)
                        if sr:
                            self.out_ports[port_name][-1] = await self.con_man.try_connect(sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                               retry_secs=1)
                else:
                    sr = data.get("sr", None)
                    sr = None if sr == "" else sr
                    self.out_ports[port_name] = sr
                    if sr:
                        self.out_ports[port_name] = await self.con_man.try_connect(sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                       retry_secs=1)

        except Exception as e:
            print(f"{os.path.basename(__file__)}: Exception connecting to ports via toml:\n{toml_config}\n Exception: {e}")

    @staticmethod
    async def create_from_toml_reader_sr(config_reader_sr: str, ins=None, outs=None, connection_manager=None):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_toml_reader_sr(config_reader_sr)
        return pc

    async def connect_from_toml_reader_sr(self, config_reader_sr: str):
        try:
            config_reader = await self.con_man.try_connect(config_reader_sr, cast_as=fbp_capnp.Channel.Reader,
                                                         retry_secs=1)
            config_msg = await config_reader.read()
            await self.connect_from_toml_str(config_msg.value.as_text())
        except Exception as e:
            print(f"{os.path.basename(__file__)}: Exception connecting to config reader via sr ({config_reader_sr}): {e}")
