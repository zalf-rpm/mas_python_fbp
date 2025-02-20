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

def start_component(path_to_executable, port_infos_reader_sr):
    proc = sp.Popen(list(path_to_executable.split(" ")) + [port_infos_reader_sr], stdout=sp.PIPE, text=True)
    return proc

class Component(fbp_capnp.Component.Server, common.Identifiable):

    def __init__(self, path_to_executable, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc = None

    async def start(self, port_infos_reader_sr, _context):  # start @0 (portInfosReaderSr :Text) -> (success :Bool);
        self.proc = start_component(self.path_to_executable, port_infos_reader_sr)
        return self.proc.poll() is None

    async def stop(self, _context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            return rt
        return False
