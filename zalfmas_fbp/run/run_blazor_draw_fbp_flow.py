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
import base64
import json
import os
import subprocess as sp
import sys
import uuid
from collections import defaultdict

import capnp
import channels as chans
from zalfmas_capnp_schemas import common_capnp, fbp_capnp
from zalfmas_common import common

# def get_free_port():
#    with socket.socket() as s:
#        s.bind(('',0))
#        return s.getsockname()[1]

standalone_config = {
    "hpc": False,
    "use_infiniband": False,
    "path_to_flow": "/home/berg/GitHub/mas_python_fbp/flows/test_load_monica_env.json",
    "path_to_channel": "/home/berg/GitHub/monica/_cmake_debug/common/channel",
    "path_to_out_dir": "/home/berg/GitHub/mas_python_fbp/out/",
}


async def start_flow_via_port_infos_sr(config: dict):
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    # use_infiniband = config["use_infiniband"]
    # node_hostname = socket.gethostname()
    # if config["use_infiniband"]:
    #    node_hostname.replace(".service", ".opa")
    # node_ip = socket.gethostbyname(node_hostname)

    # read flow file
    # flow_json = None
    with open(config["path_to_flow"]) as _:
        flow_json = json.load(_)

    if not flow_json:
        print(f"{os.path.basename(__file__)} error: could not read flow file")
        sys.exit(1)

    # create dicts for easy access to nodes and links
    node_id_to_node = {
        node["nodeId"]: node for node in flow_json["nodes"] if "nodeId" in node
    }
    links = [
        {"out": link["source"], "in": link["target"]} for link in flow_json["links"]
    ]

    # create config IIPs if a node has a defaultConfig but no connected config port
    node_id_to_config = {
        node["nodeId"]: {"config": node["config"], "generate_iip": True}
        for node in flow_json["nodes"]
        if "config" in node
    }
    for link in flow_json["links"]:
        tc = node_id_to_config.get(link["target"]["nodeId"], None)
        if tc and link["target"]["port"] == "conf":
            tc["generate_iip"] = False
    for node_id, conf in node_id_to_config.items():
        if conf["generate_iip"]:
            iip_id = str(uuid.uuid4())
            node_id_to_node[iip_id] = {
                "nodeId": iip_id,
                "componentId": "iip",
                "content": conf["config"],
            }
            links.append(
                {
                    "out": {"nodeId": iip_id, "port": "Right"},
                    "in": {"nodeId": node_id, "port": "conf"},
                }
            )

    # read path file(s)
    path_to_cmds = flow_json["cmds"]
    if type(path_to_cmds) is str:
        path_to_cmds = [path_to_cmds]
    component_id_to_cmd = {}
    # create dict for easy access to cmds
    for ptc in path_to_cmds:
        with open(ptc) as _:
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
        component_id = node.get("componentId", None)
        if "component" in node:
            cmd = node["component"]["cmd"]
        elif component_id:
            cmd = component_id_to_cmd.get(component_id, None)
        if not cmd:
            continue

        if "parallelProcesses" in node:
            process_id_to_parallel_count[node_id] = node["parallelProcesses"]
        process_id_to_popen_args[node_id] = cmd.split(" ")

    process_id_to_process = {}
    channels = []

    try:
        first_chan, first_reader_sr, first_writer_sr = chans.start_first_channel(
            config["path_to_channel"]
        )
        channels.append(first_chan)

        con_man = common.ConnectionManager()
        first_reader = await con_man.try_connect(
            first_reader_sr, cast_as=fbp_capnp.Channel.Reader
        )

        process_id_to_process_srs = defaultdict(lambda: defaultdict(dict))
        # chan_id_to_in_out_sr_names = {}

        # start all channels for the flow
        for link in links:
            out_port = link["out"]
            in_port = link["in"]
            chan_id = (
                base64.urlsafe_b64encode(
                    json.dumps(
                        {
                            "out": {
                                "nodeId": out_port["nodeId"],
                                "port": out_port["port"],
                            },
                            "in": {
                                "nodeId": in_port["nodeId"],
                                "port": in_port["port"],
                            },
                        }
                    ).encode()
                )
                .decode("ascii")
                .rstrip("=")
            )
            # start channel
            chan = chans.start_channel(
                config["path_to_channel"], chan_id, first_writer_sr, name=chan_id
            )
            channels.append(chan)

            process_id_to_process_srs[out_port["nodeId"]]["outPorts"][
                out_port["port"]
            ] = None
            process_id_to_process_srs[in_port["nodeId"]]["inPorts"][in_port["port"]] = (
                None
            )

        # start one channel for the configuration of each component (as many channels in one channel service)
        no_of_components = len(process_id_to_popen_args)
        config_chan_id = str(uuid.uuid4())
        channels.append(
            chans.start_channel(
                config["path_to_channel"],
                config_chan_id,
                first_writer_sr,
                no_of_channels=no_of_components,
                name=config_chan_id,
            )
        )
        config_chans = []
        port_infos_writers = []
        sink_process_ids = []

        async def check_and_run_process(process_id):
            io_to_srs = process_id_to_process_srs[process_id]
            if len(config_chans) > 0 and all(
                [sr is not None for srs in io_to_srs.values() for _, sr in srs.items()]
            ):
                port_infos_msg = fbp_capnp.PortInfos.new_message()
                in_ports = list(
                    [
                        {"name": name, "sr": sr}
                        for name, sr in io_to_srs["inPorts"].items()
                    ]
                )
                out_ports = list(
                    [
                        {"name": name, "sr": sr}
                        for name, sr in io_to_srs["outPorts"].items()
                    ]
                )
                if len(out_ports) == 0:
                    sink_process_ids.append(process_id)
                port_infos_msg.inPorts = in_ports
                port_infos_msg.outPorts = out_ports
                config_srs = config_chans.pop()
                for i in range(process_id_to_parallel_count.get(process_id, 1)):
                    process_id_to_process[process_id] = sp.Popen(
                        process_id_to_popen_args[process_id] + [config_srs["readerSR"]]
                    )
                    # connect to the current components config channel and send port information, then close it
                    port_infos_writer = await con_man.try_connect(
                        config_srs["writerSR"], cast_as=fbp_capnp.Channel.Writer
                    )
                    await port_infos_writer.write(value=port_infos_msg)
                    # don't close port infos writer channel, but use it as signal for letting
                    # the component close down, because it has to stay alive to forward cap calls
                    port_infos_writers.append(port_infos_writer)
                    # await port_infos_writer.close()
                    # del port_infos_writer

        # collect channel sturdy refs to start components
        no_of_startup_infos_still_to_be_received = no_of_components + len(links)
        while no_of_startup_infos_still_to_be_received > 0:
            p = (await first_reader.read()).value.as_struct(common_capnp.Pair)
            chan_id = p.fst.as_text()
            info = p.snd.as_struct(fbp_capnp.Channel.StartupInfo)
            no_of_startup_infos_still_to_be_received -= 1
            if chan_id == config_chan_id:
                out_process_id = None
                config_chans.append(
                    {"readerSR": info.readerSRs[0], "writerSR": info.writerSRs[0]}
                )
            else:

                def decode_no_padding(encoded_str):
                    # Calculate how much padding is needed
                    missing_padding = len(encoded_str) % 4
                    if missing_padding:
                        encoded_str += "=" * (4 - missing_padding)
                    return base64.urlsafe_b64decode(encoded_str)

                chan_id = json.loads(decode_no_padding(chan_id).decode())
                out_process_id = chan_id["out"]["nodeId"]
                out_port_name = chan_id["out"]["port"]
                in_process_id = chan_id["in"]["nodeId"]
                in_port_name = chan_id["in"]["port"]

                if out_process_id in iip_process_ids:
                    out_writer = await con_man.try_connect(
                        info.writerSRs[0], cast_as=fbp_capnp.Channel.Writer
                    )
                    content = node_id_to_node[out_process_id]["content"]
                    out_ip = fbp_capnp.IP.new_message(content=content)
                    await out_writer.write(value=out_ip)
                    await out_writer.write(done=None)
                    await out_writer.close()
                    del out_writer
                    # not needed anymore since we sent the IIP
                    del process_id_to_process_srs[out_process_id]
                else:
                    process_id_to_process_srs[out_process_id]["outPorts"][
                        out_port_name
                    ] = info.writerSRs[0]
                process_id_to_process_srs[in_process_id]["inPorts"][in_port_name] = (
                    info.readerSRs[0]
                )

        for process_id in process_id_to_popen_args.keys():
            await check_and_run_process(process_id)

        for process_id in sink_process_ids:  # process_id_to_process.values():
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


if __name__ == "__main__":
    asyncio.run(capnp.run(start_flow_via_port_infos_sr(standalone_config)))
