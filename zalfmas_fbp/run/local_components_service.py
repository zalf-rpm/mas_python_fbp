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
from collections import defaultdict
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
import registry_capnp

class Runnable(fbp_capnp.Component.Runnable.Server, common.Identifiable):
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


class Service(registry_capnp.Registry.Server, common.Identifiable, common.Persistable):

    def __init__(self, components: dict, cmds: dict,
                 id=None, name=None, description=None, restorer=None):
        common.Persistable.__init__(self, restorer)
        common.Identifiable.__init__(self, id, name, description)

        self._components = components
        self._cat_id_to_component_holders = defaultdict(list)
        self._cmds = cmds

        for e in self._components["entries"]:
            c = e["component"]
            info = c["info"]
            c_id = info["id"]
            if c_id in self._cmds:
                c["run"] = Runnable(self._cmds[c_id], c_id, info.get("name", None), info.get("description", None))
            self._cat_id_to_component_holders[e["categoryId"]].append(registry_capnp.Registry.Entry.new_message(
                categoryId=e["categoryId"],
                ref=common.IdentifiableHolder(fbp_capnp.Component.new_message(**c)),
                id=c_id,
                name=info.get("name", c_id)
            ))

    async def supportedCategories_context(self, context):  # supportedCategories @0 () -> (cats :List(IdInformation));
        context.results.cats = self._components["categories"]

    async def categoryInfo_context(self, context):  # categoryInfo @1 (categoryId :Text) -> IdInformation;
        cat_id = context.params.categoryId
        r = context.results
        for c in self._components["categories"]:
            if c["id"] == cat_id:
                r.id = c["id"]
                r.name = c.get("name", r.id)
                if "description" in c:
                    r.description = c["description"]

    async def entries_context(self, context):  # entries @2 (categoryId :Text) -> (entries :List(Entry));
        cat_id = context.params.categoryId
        r = context.results
        if cat_id in self._cat_id_to_component_holders:
            chs = self._cat_id_to_component_holders[cat_id]
        else:
            chs = []
            for _, v in self._cat_id_to_component_holders.items():
                chs.extend(v)
        r.init("entries", len(chs))
        for i, ch in enumerate(chs):
            r.entries[i] = ch

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
