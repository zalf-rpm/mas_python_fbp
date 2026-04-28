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
import copy
import json
import logging
import os.path
import subprocess as sp
import sys
from collections import Counter, defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.registry import registry_capnp
from zalfmas_common import common
from zalfmas_common import service as serv

import zalfmas_fbp.run.components as comp
import zalfmas_fbp.run.process as process

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ProcessClient


class Runnable(fbp_capnp.Runnable.Server, common.Identifiable):
    def __init__(
        self,
        path_to_executable,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable = path_to_executable
        self.proc: sp.Popen[str] | sp.Popen[bytes] | None = None
        self.stopped_callbacks = []

    async def start_context(
        self, context
    ):  # start @0 (portInfosReaderSr :SturdyRef, name :Text, stoppedCb :StoppedCallback) -> (success :Bool);
        port_infos_reader_sr_str = common.sturdy_ref_str_from_sr(context.params.portInfosReaderSr)
        name = context.params.name
        if context.params._has("stoppedCb"):
            self.stopped_callbacks.append(context.params.stoppedCb)
        self.proc = comp.start_local_component(self.path_to_executable, port_infos_reader_sr_str, name)
        context.results.success = self.proc.poll() is None

    @override
    async def stop_context(self, context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            context.results.success = rt
            for scb in self.stopped_callbacks:
                await scb.stopped()
        context.results.success = True


class RunnableFactory(fbp_capnp.Runnable.Factory.Server, common.Identifiable):
    def __init__(
        self,
        path_to_executable: str,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable: str = path_to_executable
        self.runnables: list[Runnable] = []
        self.count: int = 0

    # create @0 () -> (r :Runnable);
    @override
    async def create(self, _context, **kwargs):
        self.count += 1
        r = Runnable(
            self.path_to_executable,
            id=f"{self.id}_{self.count}",
            name=f"{self.name} {self.count}",
            description=self.description,
        )
        self.runnables.append(r)
        return r


class ProcessWriter(fbp_capnp.Channel.Writer.Server):
    def __init__(self):
        self.process_cap: ProcessClient | None = None
        self.process_cap_received_future = asyncio.Future()
        self.unregister_writer: Callable[..., None] | None = None

    # struct Msg {
    #   union {
    #     value @0 :V;
    #     done  @1 :Void;   # done message, no more data will be sent (indicate upstream is done - but semantics up to user)
    #     noMsg @2 :Void;   # no message available, if readIfMsg is used
    #   }
    # }
    # write @0 Msg;
    @override
    async def write_context(self, context):
        if context.params.which() == "value":
            self.process_cap = context.params.value.as_interface(fbp_capnp.Process)
            self.process_cap_received_future.set_result(self.process_cap)
            if self.unregister_writer:
                await self.unregister_writer()


class ProcessFactory(fbp_capnp.Process.Factory.Server, common.Identifiable):
    def __init__(
        self,
        path_to_executable: str,
        restorer: common.Restorer,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.path_to_executable: str = path_to_executable
        self.procs: list[sp.Popen[str]] = []
        # self.proc_writers = []
        self.count: int = 0
        self.restorer: common.Restorer = restorer

    def __del__(self):
        for proc in self.procs:
            proc.terminate()

    # create @0 () -> (r :Process);
    @override
    async def create(self, _context, **kwargs):
        self.count += 1
        writer = ProcessWriter()
        save_sr_token, unsave_sr_token = await self.restorer.save_cap(writer)

        async def unsave():
            if unsave_sr_token:
                _ = await self.restorer.unsave(unsave_sr_token)

        writer.unregister_writer = unsave  # lambda: self.restorer.unsave(unsave_sr_token)
        writer_sr_str = self.restorer.sturdy_ref_str(save_sr_token)
        self.procs.append(process.start_local_process_component(self.path_to_executable, writer_sr_str, self.id))
        process_cap = await writer.process_cap_received_future
        return process_cap


class Service(registry_capnp.Registry.Server, common.Identifiable, common.Persistable):
    def __init__(
        self,
        cat_id_to_name_and_component_holders: dict[str, Any],
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        restorer: common.Restorer | None = None,
    ):
        common.Persistable.__init__(self, restorer)
        common.Identifiable.__init__(self, id, name, description)

        self._cat_id_to_name_and_component_holders = cat_id_to_name_and_component_holders

    async def supportedCategories_context(self, context):  # supportedCategories @0 () -> (cats :List(IdInformation));
        cats = list(
            [{"id": cat_id, "name": v["name"]} for cat_id, v in self._cat_id_to_name_and_component_holders.items()]
        )
        context.results.cats = cats

    async def categoryInfo_context(self, context):  # categoryInfo @1 (categoryId :Text) -> IdInformation;
        cat_id = context.params.categoryId
        r = context.results
        if n_to_chs := self._cat_id_to_name_and_component_holders.get(cat_id):
            r.id = cat_id
            r.name = n_to_chs.get("name", r.id)

    async def entries_context(self, context):  # entries @2 (categoryId :Text) -> (entries :List(Entry));
        cat_id = context.params.categoryId
        r = context.results
        if n_to_chs := self._cat_id_to_name_and_component_holders.get(cat_id):
            chs = n_to_chs.get("component_holders")
        else:
            chs = []
            for _, v in self._cat_id_to_name_and_component_holders.items():
                chs.extend(v.get("component_holders", []))
        r.init("entries", len(chs))
        for i, ch in enumerate(chs):
            r.entries[i] = ch


def load_component_metadata(cmds: dict[str, str], components_cache: dict[str, Any], restorer: common.Restorer):
    cat_id_to_name_and_component_holders = defaultdict(lambda: {"name": [], "component_holders": []})
    for comp_id, cmd_str in cmds.items():
        if comp_id == "id" or comp_id == "name" or comp_id[:3] == "___":
            continue

        comp_in_cache = components_cache and comp_id in components_cache
        meta: dict[str, Any] | None = copy.deepcopy(components_cache[comp_id]) if comp_in_cache else None
        if meta is None:
            try:
                pte_split = list(cmd_str.split(" "))
                if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
                    pte_split[0] = sys.executable
                res = sp.run(pte_split + ["-O"], stdout=sp.PIPE, text=True)
                if res is None:
                    continue
                meta = json.loads(res.stdout)
                components_cache[comp_id] = copy.deepcopy(meta)
            except (json.JSONDecodeError, OSError, RuntimeError, sp.SubprocessError, TypeError, ValueError) as e:
                logger.warning("Couldn't execute component via '%s'. Exception: %s", pte_split + ["-O"], e)

        try:
            c = meta["component"]
            info = c["info"]
            c_id = info["id"]
            if c_id != comp_id:
                logger.warning(
                    "Component id=%s in cmds is not the same as in referenced component (id=%s)! Skipping component.",
                    comp_id,
                    c_id,
                )
                continue

            if "defaultConfig" in c:
                dc_json = json.dumps(c["defaultConfig"])
                c["defaultConfig"] = common_capnp.StructuredText.new_message(type="json", value=dc_json)

            if c_id in cmds:
                c["factory"] = {}
                if c["type"] == "standard":
                    c["factory"]["runnable"] = RunnableFactory(
                        cmd_str,
                        id=c_id,
                        name=info.get("name", None),
                        description=info.get("description", None),
                    )
                elif c["type"] == "process":
                    c["factory"]["process"] = ProcessFactory(
                        cmd_str,
                        restorer=restorer,
                        id=c_id,
                        name=info.get("name", None),
                        description=info.get("description", None),
                    )
            cat_id = meta["category"]["id"]
            cat_name = meta["category"].get("name", cat_id)
            cat_id_to_name_and_component_holders[cat_id]["name"].append(cat_name)
            cat_id_to_name_and_component_holders[cat_id]["component_holders"].append(
                registry_capnp.Registry.Entry.new_message(
                    categoryId=cat_id,
                    ref=common.IdentifiableHolder(fbp_capnp.Component.new_message(**c)),
                    id=c_id,
                    name=info.get("name", c_id),
                )
            )

        except (capnp.KjException, KeyError, TypeError, ValueError) as e:
            logger.warning(
                "Some exception happend during retrieving metadata for component with id=%s. Exception: %s",
                comp_id,
                e,
            )

    # if there are multiple names for the same category, use the one that appears most
    for cat_id, n_to_cs in cat_id_to_name_and_component_holders.items():
        if len(n_to_cs["name"]) > 1:
            mc = Counter(n_to_cs["name"]).most_common(1)
            cat_id_to_name_and_component_holders[cat_id]["name"] = (
                mc[0][0] if len(mc) > 0 and len(mc[0]) > 0 else "unknown"
            )
        else:
            assert len(n_to_cs["name"]) == 1
            cat_id_to_name_and_component_holders[cat_id]["name"] = n_to_cs["name"][0]

    return cat_id_to_name_and_component_holders


async def main():
    parser = serv.create_default_args_parser(
        component_description="local FBP component start service",
        default_config_path="./configs/local_components_service.toml",
    )
    config, args = serv.handle_default_service_args(parser, path_to_service_py=__file__)

    # load components cache
    if os.path.exists(config["service"]["path_to_components_cache_json"]):
        with open(config["service"]["path_to_components_cache_json"]) as f:
            components_cache = json.load(f)
    else:
        components_cache = {}
    with open(config["service"]["path_to_cmds_json"]) as f:
        cmds = json.load(f)

    cs = config.get("service", {})
    restorer = common.Restorer()

    cat_id_to_name_and_component_holders = load_component_metadata(cmds, components_cache, restorer)
    # update components cache
    with open(config["service"]["path_to_components_cache_json"], "w") as f:
        json.dump(components_cache, f, indent=4)

    service = Service(
        cat_id_to_name_and_component_holders,
        id=cs.get("id", None),
        name=cs.get("name", None),
        description=cs.get("description", None),
        restorer=restorer,
    )
    await serv.init_and_run_service_from_config(config=config, service=service, restorer=restorer)


if __name__ == "__main__":
    asyncio.run(capnp.run(main()))
