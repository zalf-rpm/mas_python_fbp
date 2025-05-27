#!/usr/bin/python
# -*- coding: UTF-8

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

import asyncio
import capnp
import json
import os
import sys
import uuid
from zalfmas_common import common
from zalfmas_common import service as serv
import run.components as comp
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp


class Component(fbp_capnp.Component.Runnable.Server, common.Identifiable):
    def __init__(self, path_to_executable, id=None, name=None, description=None, admin=None, restorer=None):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc = None

    async def start_context(self, context):  # start @0 (portInfosReaderSr :Text) -> (success :Bool);
        port_infos_reader_sr = context.params.portInfosReaderSr
        self.proc = comp.start_local_component(self.path_to_executable, port_infos_reader_sr)
        context.results.success = self.proc.poll() is None

    async def stop_context(self, context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            context.results.success = rt
        context.results.success = False


class Service(fbp_capnp.ComponentService.Server, common.Identifiable, common.Persistable):

    def __init__(self, components: dict, cmds: dict,
                 id=None, name=None, description=None, restorer=None):
        common.Persistable.__init__(self, restorer)
        common.Identifiable.__init__(self, id, name, description)

        self._components = components
        self._cmds = cmds

        for c in [e["component"] for e in self._components["entries"]]:
            info = c["info"]
            c_id = info["id"]
            if c_id in self._cmds:
                c["run"] = Component(self._cmds[c_id], c_id, info.get("name", None), info.get("description", None))


    async def categories_context(self, context):  # categories  @2 () -> (categories :List(Common.IdInformation));
        r = context.results
        cats = self._components["categories"]
        r.init("categories", len(cats))
        for i, c in enumerate(cats):
            r.categories[i] = c

    async def list_context(self, context):  # list @0 () -> (entries :List(Entry));
        r = context.results
        entries = self._components["entries"]
        r.init("entries", len(entries))
        for i, e in enumerate(entries):
            r.entries[i] = e

    async def component_context(self, context):  # component @1 (id :Text) -> Component;
        comp_id = context.params.id
        if not comp_id:
            return
        r = context.results
        comps = [e for e in self._components["entries"] if e["component"]["info"]["id"] == comp_id]
        if len(comps) == 0:
            return
        r.comp = comps[0]["component"]


default_config = {
    "id": str(uuid.uuid4()),
    "name": "local FBP components service",
    "description": None,
    "path_to_components_json": "path to components.json here",
    "path_to_cmds_json": "path to cmds.json here",
    "host": None,
    "port": None,
    "serve_bootstrap": True,
    "fixed_sturdy_ref_token": None,
    "reg_sturdy_ref": None,
    "reg_category": None,

    "opt:id": "ID of the service",
    "opt:name": "local FBP components service",
    "opt:description": "Serves locally startable components",
    "opt:path_to_components_json": "[string (path)] -> Path to JSON file holding the data for the available components",
    "opt:path_to_cmds_json": "[string (path)] -> Path to JSON containing the mapping of component id to commandline execution",
    "opt:host": "[string (IP/hostname)] -> Use this host (e.g. localhost)",
    "opt:port": "[int] -> Use this port (missing = default = choose random free port)",
    "opt:serve_bootstrap": "[true | false] -> Is the service reachable directly via its restorer interface",
    "opt:fixed_sturdy_ref_token": "[string] -> Use this token as the sturdy ref token of this service",
    "opt:reg_sturdy_ref": "[string (sturdy ref)] -> Connect to registry using this sturdy ref",
    "opt:reg_category": "[string] -> Connect to registry using this category",
}
async def main():
    parser = serv.create_default_args_parser("local FBP component start service")
    config, args = serv.handle_default_service_args(parser, default_config)

    with open(config["path_to_components_json"], "r") as f:
        components = json.load(f)
    with open(config["path_to_cmds_json"], "r") as f:
        cmds = json.load(f)

    restorer = common.Restorer()
    service = Service(components, cmds,
                      id=config["id"], name=config["name"], description=config["description"],
                      restorer=restorer)
    await serv.init_and_run_service({"service": service},
                                    config["host"], config["port"],
                                    serve_bootstrap=config["serve_bootstrap"],
                                    name_to_service_srs={"service": config["fixed_sturdy_ref_token"]},
                                    restorer=restorer)

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))
