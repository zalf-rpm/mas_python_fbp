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
from pathlib import Path
import subprocess as sp
import sys
import uuid
from zalfmas_common import common
from zalfmas_common import service as serv
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp

def start_component(path_to_executable, port_infos_reader_sr):
    proc = sp.Popen(list(path_to_executable.split(" ")) + [port_infos_reader_sr],
                    #stdout=sp.PIPE, stderr=sp.STDOUT,
                    text=True)
    return proc

class Component(fbp_capnp.Component.Server, common.Identifiable):

    def __init__(self, path_to_executable, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc = None

    async def start(self, portInfosReaderSr, _context):  # start @0 (portInfosReaderSr :Text) -> (success :Bool);
        self.proc = start_component(self.path_to_executable, portInfosReaderSr)
        return self.proc.poll() is None

    async def stop(self, _context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            return rt
        return False

async def main(path_to_executable=None, serve_bootstrap=True, host=None, port=None,
               id=None, name="FBP component service", description=None, srt=None):
    config = {
        "path_to_executable": path_to_executable,
        "port": port,
        "host": host,
        "id": id,
        "name": name,
        "description": description,
        "serve_bootstrap": serve_bootstrap,
        "srt": srt
    }
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    restorer = common.Restorer()
    service = Component(config["path_to_executable"], id, name, description)
    name_to_service = {"service": service}
    await serv.init_and_run_service(name_to_service, config["host"], config["port"],
                                    serve_bootstrap=config["serve_bootstrap"], restorer=restorer,
                                    name_to_service_srs={"service": config["srt"]})

if __name__ == '__main__':
    asyncio.run(capnp.run(main(serve_bootstrap=True)))