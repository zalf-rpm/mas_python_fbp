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
from pathlib import Path
import subprocess as sp
import sys
import uuid
from zalfmas_common import common
from zalfmas_common import fbp
import channels as chans
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp
import common_capnp

#def get_free_port():
#    with socket.socket() as s:
#        s.bind(('',0))
#        return s.getsockname()[1]

standalone_config = {
    "hpc": False,
    "use_infiniband": False,
    "path_to_flow": "/home/berg/Downloads/calibration_4.json",
    "path_to_channel": "/home/berg/GitHub/monica/_cmake_debug/common/channel",
    "path_to_out_dir": "/home/berg/GitHub/mas_python_fbp/out/",
}
async def main(config: dict):
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    #use_infiniband = config["use_infiniband"]
    #node_hostname = socket.gethostname()
    #if config["use_infiniband"]:
    #    node_hostname.replace(".service", ".opa")
    #node_ip = socket.gethostbyname(node_hostname)

    # read flow file
    #flow_json = None
    with open(config["path_to_flow"], "r") as _:
        flow_json = json.load(_)

    if not flow_json:
        print(f"{os.path.basename(__file__)} error: could not read flow file")
        sys.exit(1)

    # create dicts for easy access to nodes and links
    node_id_to_node = {node["node_id"]: node for node in flow_json["nodes"]}
    links = [{"out": link["source"], "in": link["target"]} for link in flow_json["links"]]

    # read components file
    path_to_components = flow_json["components"]
    #component_id_to_component = None
    # create dicts for easy access to components
    with open(path_to_components, "r") as _:
        component_id_to_component = {
            component["id"]: component
            for cat, comps in json.load(_).items()
            for component in comps
        }

    # a mapping of node_id to lambdas for process creation
    process_id_to_Popen_args = {}
    process_id_to_parallel_count = {}
    iip_process_ids = set()
    for node_id, node in node_id_to_node.items():
        component = component_id_to_component.get(node["component_id"], None) if "component_id" in node else None
        if not component:
            component = node["inline_component"]
        if not component:
            continue
        # we will send IIPs directly on the channel to the receiving process
        if component.get("type", "") == "CapnpFbpIIP":
            iip_process_ids.add(node_id)
            continue
        if "parallel_processes" in node:
            process_id_to_parallel_count[node_id] = node["parallel_processes"]
        # args
        args = []
        if "interpreter" in component and len(component["interpreter"]) > 0:
            args.append(component["interpreter"])
        args.append(component["path"])
        for k, v in node["data"]["cmd_params"].items():
            args.append(f"--{k}={v}")
        process_id_to_Popen_args[node_id] = args

    process_id_to_process = {}
    channels = []

    try:
        first_chan, first_reader_sr, first_writer_sr = chans.start_first_channel(config["path_to_channel"])
        channels.append(first_chan)

        con_man = common.ConnectionManager()
        first_reader = await con_man.try_connect(first_reader_sr, cast_as=fbp_capnp.Channel.Reader)

        process_id_to_process_srs = defaultdict(dict)
        chan_id_to_in_out_sr_names = {}

        # start all channels for the flow
        for link in links:
            out_port = link["out"]
            in_port = link["in"]
            chan_id = f"{out_port['node_id']}.{out_port['port']}->{in_port['node_id']}.{in_port['port']}"
            # start channel
            chan = chans.start_channel(config["path_to_channel"], chan_id + "|" + first_writer_sr, name=chan_id)
            channels.append(chan)

            chan_id_to_in_out_sr_names[chan_id] = {"out": out_port['port'], "in": in_port['port']}
            process_id_to_process_srs[out_port["node_id"]][out_port["port"]] = None
            process_id_to_process_srs[in_port["node_id"]][in_port["port"]] = None

        # collect channel sturdy refs in order to start components
        while True:
            p = (await first_reader.read()).value.as_struct(common_capnp.Pair)
            out_process_and_port, in_process_and_port = p.fst.as_text().split("->")
            out_process_id, out_port_name = out_process_and_port.split(".")
            in_process_id, in_port_name = in_process_and_port.split(".")

            # there should be code to start the components
            if ((out_process_id in process_id_to_Popen_args or out_process_id in iip_process_ids)
                    and in_process_id in process_id_to_Popen_args):
                info = p.snd.as_struct(fbp_capnp.Channel.StartupInfo)

                if out_process_id in iip_process_ids:
                    out_writer = await con_man.try_connect(info.writerSRs[0], cast_as=fbp_capnp.Channel.Writer)
                    content = node_id_to_node[out_process_id]["data"]["content"]
                    out_ip = fbp_capnp.IP.new_message(content=content)
                    await out_writer.write(value=out_ip)
                    await out_writer.write(done=None)
                    await out_writer.close()
                    del out_writer
                    # not needed anymore since we sent the IIP
                    del process_id_to_process_srs[out_process_id]
                else:
                    process_id_to_process_srs[out_process_id][out_port_name] = \
                        f"--{out_port_name}_out_sr={info.writerSRs[0]}".replace('out_out_', 'out_')
                process_id_to_process_srs[in_process_id][in_port_name] = \
                    f"--{in_port_name}_in_sr={info.readerSRs[0]}".replace('in_in_', 'in_')

                # sturdy refs for all ports of start component are available
                # check for a non IIP process_id if the process can be started
                if out_process_id not in iip_process_ids:
                    srs = process_id_to_process_srs[out_process_id].values()
                    if all([sr is not None for sr in srs]):
                        for i in range(process_id_to_parallel_count.get(out_process_id, 1)):
                            process_id_to_process[out_process_id] = sp.Popen(process_id_to_Popen_args[out_process_id] + list(srs))

                # sturdy refs for all ports of end component are available
                srs = process_id_to_process_srs[in_process_id].values()
                if all([sr is not None for sr in srs]):
                    for i in range(process_id_to_parallel_count.get(in_process_id, 1)):
                        process_id_to_process[in_process_id] = sp.Popen(process_id_to_Popen_args[in_process_id] + list(srs))

                # exit loop if we started all components in the flow
                if len(process_id_to_process) == len(process_id_to_process_srs):
                    break

        for process in process_id_to_process.values():
            process.wait()

        print(f"{os.path.basename(__file__)}: all components finished")

        for channel in channels:
            channel.terminate()
        print(f"{os.path.basename(__file__)}: all channels terminated")

    except Exception as e:
        for process in process_id_to_process.values():
            process.terminate()

        for channel in channels:
            channel.terminate()

        print(f"exception terminated {os.path.basename(__file__)} early. Exception:", e)

if __name__ == '__main__':
    asyncio.run(capnp.run(main(standalone_config)))