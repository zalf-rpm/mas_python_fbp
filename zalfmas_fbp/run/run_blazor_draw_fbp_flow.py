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
import channels as chans
import fbp_capnp
import common_capnp

#def get_free_port():
#    with socket.socket() as s:
#        s.bind(('',0))
#        return s.getsockname()[1]

standalone_config = {
    "hpc": False,
    "use_infiniband": False,
    "path_to_flow": "/home/berg/GitHub/mas_python_fbp/test_flow3.json",
    "path_to_channel": "/home/berg/GitHub/monica/_cmake_debug/common/channel",
    "path_to_out_dir": "/home/berg/GitHub/mas_python_fbp/out/",
}
async def start_flow_via_port_infos_sr(config: dict):
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

    # read components file(s)
    #path_to_components = flow_json["components"]
    #if type(path_to_components) is str:
    #    path_to_components = [path_to_components]
    #component_id_to_component = {}
    ## create dicts for easy access to components
    #for ptcs in path_to_components:
    #    with open(ptcs, "r") as _:
    #        component_id_to_component.update({
    #            e["component"]["info"]["id"]: e["component"]
    #            for e in json.load(_)["entries"]
    #        })

    # read path file(s)
    path_to_cmds = flow_json["cmds"]
    if type(path_to_cmds) is str:
        path_to_cmds = [path_to_cmds]
    component_id_to_cmd = {}
    # create dict for easy access to cmds
    for ptc in path_to_cmds:
        with open(ptc, "r") as _:
            component_id_to_cmd.update(json.load(_))

    # a mapping of node_id to lambdas for process creation
    process_id_to_popen_args = {}
    process_id_to_parallel_count = {}
    iip_process_ids = set()
    for node_id, node in node_id_to_node.items():
        # is it an IIP
        # we will send IIPs directly on the channel to the receiving process
        if "content" in node:
            iip_process_ids.add(node_id)
            continue

        cmd = None
        component_id = node.get("component_id", None)
        if "inline_component" in node:
            cmd = node["inline_component"]["cmd"]
        elif component_id:
            cmd = component_id_to_cmd.get(component_id, None)
        if not cmd:
            continue

        if "parallel_processes" in node:
            process_id_to_parallel_count[node_id] = node["parallel_processes"]
        process_id_to_popen_args[node_id] = cmd.split(" ")

    process_id_to_process = {}
    channels = []

    try:
        first_chan, first_reader_sr, first_writer_sr = chans.start_first_channel(config["path_to_channel"])
        channels.append(first_chan)

        con_man = common.ConnectionManager()
        first_reader = await con_man.try_connect(first_reader_sr, cast_as=fbp_capnp.Channel.Reader)

        process_id_to_process_srs = defaultdict(lambda: defaultdict(dict))
        #chan_id_to_in_out_sr_names = {}

        # start all channels for the flow
        for link in links:
            out_port = link["out"]
            in_port = link["in"]
            chan_id = f"{out_port['node_id']}.{out_port['port']}->{in_port['node_id']}.{in_port['port']}"
            # start channel
            chan = chans.start_channel(config["path_to_channel"], chan_id, first_writer_sr, name=chan_id)
            channels.append(chan)

            process_id_to_process_srs[out_port["node_id"]]["out_ports"][out_port["port"]] = None
            process_id_to_process_srs[in_port["node_id"]]["in_ports"][in_port["port"]] = None

        # start one channel for the configuration of each component (as many channels in one channel service)
        no_of_components = len(process_id_to_popen_args)
        config_chan_id = str(uuid.uuid4())
        channels.append(chans.start_channel(config["path_to_channel"], config_chan_id, first_writer_sr,
                                            no_of_channels=no_of_components, name=config_chan_id))
        config_chans = []
        port_infos_writers = []
        sink_process_ids = []
        async def check_and_run_process(process_id):
            io_to_srs = process_id_to_process_srs[process_id]
            if len(config_chans) > 0 and all([sr is not None for srs in io_to_srs.values() for _, sr in srs.items()]):
                port_infos_msg = fbp_capnp.PortInfos.new_message()
                in_ports = list([{"name": name, "sr": sr} for name, sr in io_to_srs["in_ports"].items()])
                out_ports = list([{"name": name, "sr": sr} for name, sr in io_to_srs["out_ports"].items()])
                if len(out_ports) == 0:
                    sink_process_ids.append(process_id)
                port_infos_msg.inPorts = in_ports
                port_infos_msg.outPorts = out_ports
                config_srs = config_chans.pop()
                for i in range(process_id_to_parallel_count.get(process_id, 1)):
                    process_id_to_process[process_id] = sp.Popen(
                        process_id_to_popen_args[process_id] + [config_srs["readerSR"]])
                    # connect to the current components config channel and send port information, then close it
                    port_infos_writer = await con_man.try_connect(config_srs["writerSR"],
                                                                  cast_as=fbp_capnp.Channel.Writer)
                    await port_infos_writer.write(value=port_infos_msg)
                    port_infos_writers.append(port_infos_writer)
                    #await port_infos_writer.close()
                    #del port_infos_writer

        # collect channel sturdy refs to start components
        no_of_startup_infos_still_to_be_received = no_of_components + len(links)
        while no_of_startup_infos_still_to_be_received > 0:
            p = (await first_reader.read()).value.as_struct(common_capnp.Pair)
            chan_id = p.fst.as_text()
            info = p.snd.as_struct(fbp_capnp.Channel.StartupInfo)
            no_of_startup_infos_still_to_be_received -= 1
            if chan_id == config_chan_id:
                out_process_id = None
                config_chans.append({"readerSR": info.readerSRs[0], "writerSR": info.writerSRs[0]})
            else:
                out_process_and_port, in_process_and_port = chan_id.split("->")
                out_process_id, out_port_name = out_process_and_port.split(".")
                in_process_id, in_port_name = in_process_and_port.split(".")

                if out_process_id in iip_process_ids:
                    out_writer = await con_man.try_connect(info.writerSRs[0], cast_as=fbp_capnp.Channel.Writer)
                    content = node_id_to_node[out_process_id]["content"]
                    out_ip = fbp_capnp.IP.new_message(content=content)
                    await out_writer.write(value=out_ip)
                    await out_writer.write(done=None)
                    await out_writer.close()
                    del out_writer
                    # not needed anymore since we sent the IIP
                    del process_id_to_process_srs[out_process_id]
                else:
                    process_id_to_process_srs[out_process_id]["out_ports"][out_port_name] = info.writerSRs[0]
                process_id_to_process_srs[in_process_id]["in_ports"][in_port_name] = info.readerSRs[0]

        for process_id in process_id_to_popen_args.keys():
            await check_and_run_process(process_id)

        for process_id in sink_process_ids: #process_id_to_process.values():
            p = process_id_to_process.pop(process_id)
            p.wait()

        for piw in port_infos_writers:
            piw.close()

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
    asyncio.run(capnp.run(start_flow_via_port_infos_sr(standalone_config)))