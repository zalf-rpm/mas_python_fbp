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

# remote debugging via commandline
# -m ptvsd --host 0.0.0.0 --port 14000 --wait

import asyncio
import capnp
from collections import deque, defaultdict
import json
import os
from pathlib import Path
import subprocess as sp
import sys
import uuid
from zalfmas_common import common
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

def start_component(path_to_executable, config_iip_reader_sr):
    proc = sp.Popen(list(path_to_executable.split(" ")) + [config_iip_reader_sr], stdout=sp.PIPE, text=True)
    return proc

class Component(fbp_capnp.Component.Server, common.Identifiable):

    def __init__(self, path_to_executable, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc = None

    async def start(self, config_iip_reader_sr, _context):  # start @0 (configIipReaderSr :Text) -> (success :Bool);
        self.proc = start_component(self.path_to_executable, config_iip_reader_sr)
        return self.proc.poll() is None

    async def stop(self, _context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            return rt
        return False

async def connect_ports(config: dict, connection_manager=None):
    if not connection_manager:
        connection_manager = common.ConnectionManager()

    ports = {}
    out_ports = {}

    for k, v in config.items():
        if k.endswith("in_sr"):
            port_name = k[:-6]
            if len(port_name) == 0:
                port_name = "in"
            elif port_name[:-1] == "_":
                port_name = port_name[:-1]
            if v is None:
                ports[port_name] = None
            else:
                ports[port_name] = await connection_manager.try_connect(v, cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
        elif k.endswith("out_sr"):
            port_name = k[:-7]
            if len(port_name) == 0:
                port_name = "out"
            elif port_name[:-1] == "_":
                port_name = port_name[:-1]
            if v is None:
                ports[port_name] = None
            else:
                ports[port_name] = await connection_manager.try_connect(v, cast_as=fbp_capnp.Channel.Writer, retry_secs=1)
                out_ports[port_name] = ports[port_name]
        elif k.endswith("out_srs"):
            port_name = k[:-8]
            if len(port_name) == 0:
                port_name = "out"
            elif port_name[:-1] == "_":
                port_name = port_name[:-1]
            if v is None:
                ports[port_name] = None
            else:
                ports[port_name] = []
                for out_sr in v.split("|"):
                    ports[port_name].append(await connection_manager.try_connect(out_sr, cast_as=fbp_capnp.Channel.Writer,
                                                                                 retry_secs=1))
                out_ports[port_name] = ports[port_name]

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

    return ports, close_ports



