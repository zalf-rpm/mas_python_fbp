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


async def connect_ports_from_cmd_config(config: dict, connection_manager=None):
    if not connection_manager:
        connection_manager = common.ConnectionManager()

    try:
        in_ports = {}
        out_ports = {}

        for k, v in config.items():
            if k.endswith("in_sr"):
                port_name = k[:-6]
                if len(port_name) == 0:
                    port_name = "in"
                elif port_name[:-1] == "_":
                    port_name = port_name[:-1]
                if v is None:
                    in_ports[port_name] = None
                else:
                    in_ports[port_name] = await connection_manager.try_connect(v, cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
            elif k.endswith("out_sr"):
                port_name = k[:-7]
                if len(port_name) == 0:
                    port_name = "out"
                elif port_name[:-1] == "_":
                    port_name = port_name[:-1]
                if v is None:
                    out_ports[port_name] = None
                else:
                    in_ports[port_name] = await connection_manager.try_connect(v, cast_as=fbp_capnp.Channel.Writer, retry_secs=1)
                    out_ports[port_name] = in_ports[port_name]
            elif k.endswith("out_srs"):
                port_name = k[:-8]
                if len(port_name) == 0:
                    port_name = "out"
                elif port_name[:-1] == "_":
                    port_name = port_name[:-1]
                if v is None:
                    out_ports[port_name] = None
                else:
                    out_ports[port_name] = []
                    for out_sr in v.split("|"):
                        out_ports[port_name].append(await connection_manager.try_connect(out_sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                     retry_secs=1))

        async def close_ports(print_info=False, print_exception=True):
            for name, ps in out_ports.items():
                # is an array out port
                if isinstance(ps, list):
                    for i, p in enumerate(ps):
                        try:
                            await p.close()
                            if print_info:
                                print(f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'")
                        except Exception as e:
                            if print_exception:
                                print(f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}")
                # is a single out port
                else:
                    try:
                        await ps.close()
                        if print_info:
                            print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                    except Exception as e:
                        if print_exception:
                            print(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")

        return in_ports, out_ports, close_ports, connection_manager
    except Exception as e:
        print(f"{os.path.basename(__file__)}: Exception connecting to ports via CMD config:\n{config}\n Exception: {e}")

    return None, None, None, None

async def connect_ports_from_port_infos_reader(port_infos_reader_sr: str, ins=None, outs=None,
                                               connection_manager=None):
    if not connection_manager:
        connection_manager = common.ConnectionManager()

    try:
        pis_reader = await connection_manager.try_connect(port_infos_reader_sr,
                                                          cast_as=fbp_capnp.Channel.Reader,
                                                          retry_secs=1)
        pis = (await pis_reader.read()).value.as_struct(fbp_capnp.PortInfos)
        in_ports = {n: None for n in ins} if ins else {}
        for n2sr in pis.inPorts:
            if len(n2sr.name) > 0:
                port_name = n2sr.name
                if n2sr.which() == "sr" and len(n2sr.sr) > 0:
                    in_ports[port_name] = await connection_manager.try_connect(n2sr.sr,
                                                                               cast_as=fbp_capnp.Channel.Reader,
                                                                               retry_secs=1)

        out_ports = {n: None for n in outs} if outs else {}
        for n2sr in pis.outPorts:
            if len(n2sr.name) > 0:
                port_name = n2sr.name
                if n2sr.which() == "sr" and len(n2sr.sr) > 0:
                    out_ports[port_name] = await connection_manager.try_connect(n2sr.sr,
                                                                                cast_as=fbp_capnp.Channel.Writer,
                                                                                retry_secs=1)
                elif len(n2sr.srs) > 0:
                    out_ports[port_name] = []
                    for sr in n2sr.srs:
                        if len(sr) > 0:
                            out_ports[port_name].append(await connection_manager.try_connect(sr,
                                                                                             cast_as=fbp_capnp.Channel.Writer,
                                                                                             retry_secs=1))

        async def close_ports(print_info=False, print_exception=True):
            for name, ps in out_ports.items():
                # is an array out port
                if isinstance(ps, list):
                    for i, p in enumerate(ps):
                        if p:
                            try:
                                await p.close()
                                if print_info:
                                    print(f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'")
                            except Exception as e:
                                if print_exception:
                                    print(f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}")
                # is a single out port
                else:
                    if ps:
                        try:
                            await ps.close()
                            if print_info:
                                print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                        except Exception as e:
                            if print_exception:
                                print(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")

        return in_ports, out_ports, close_ports, connection_manager
    except Exception as e:
        print(f"{os.path.basename(__file__)}: Exception connecting to ports via port infos reader SR:\n{port_infos_reader_sr}\n Exception: {e}")

    return None, None, None, None


async def connect_ports_from_toml_str(config_toml_str: str, connection_manager=None):
    if not connection_manager:
        connection_manager = common.ConnectionManager()

    toml_config = tomli.loads(config_toml_str)

    try:
        in_ports = {}
        out_ports = {}

        for port_name, data in toml_config["ports"]["in"].items():
            sr = data.get("sr", None)
            sr = None if sr == "" else sr
            in_ports[port_name] = sr
            if sr:
                in_ports[port_name] = await connection_manager.try_connect(sr, cast_as=fbp_capnp.Channel.Reader,
                                                                           retry_secs=1)

        for port_name, data in toml_config["ports"]["out"].items():
            if isinstance(data, list):
                out_ports[port_name] = []
                for d in data:
                    sr = d.get("sr", None)
                    sr = None if sr == "" else sr
                    out_ports[port_name].append(sr)
                    if sr:
                        out_ports[port_name][-1] = await connection_manager.try_connect(sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                        retry_secs=1)
            else:
                sr = data.get("sr", None)
                sr = None if sr == "" else sr
                out_ports[port_name] = sr
                if sr:
                    out_ports[port_name] = await connection_manager.try_connect(sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                retry_secs=1)

        async def close_ports(print_info=False, print_exception=True):
            for name, ps in out_ports.items():
                # is an array out port
                if isinstance(ps, list):
                    for i, p in enumerate(ps):
                        if p:
                            try:
                                await p.close()
                                if print_info:
                                    print(f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'")
                            except Exception as e:
                                if print_exception:
                                    print(f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}")
                # is a single out port
                else:
                    if ps:
                        try:
                            await ps.close()
                            if print_info:
                                print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                        except Exception as e:
                            if print_exception:
                                print(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")

        return in_ports, out_ports, close_ports, connection_manager
    except Exception as e:
        print(f"{os.path.basename(__file__)}: Exception connecting to ports via toml:\n{toml_config}\n Exception: {e}")

    return None, None, None, None


async def connect_ports_from_reader_sr(config_reader_sr: str, connection_manager=None):
    if not connection_manager:
        connection_manager = common.ConnectionManager()
    try:
        config_reader = await connection_manager.try_connect(config_reader_sr, cast_as=fbp_capnp.Channel.Reader,
                                                             retry_secs=1)
        config_msg = await config_reader.read()
        return connect_ports_from_toml_str(config_msg.value.as_text(), connection_manager)
    except Exception as e:
        print(f"{os.path.basename(__file__)}: Exception connecting to config reader via sr ({config_reader_sr}): {e}")

    return None, None, None, None
