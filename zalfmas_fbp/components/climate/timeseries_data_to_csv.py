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
from datetime import timedelta, date
import io
import os
import sys
from zalfmas_common import common
import zalfmas_fbp.run.ports as p
import zalfmas_fbp.run.components as c
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import fbp_capnp
import geo_capnp
import climate_capnp

async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(port_infos_reader_sr,
                                                                ins=["conf", "in"], outs=["out"])
    await p.update_config_from_port(config, ports["conf"])

    def data_to_csv(header: list, data: list[list[float]], start_date: date):
        csv_buffer = io.StringIO()
        h_str = ",".join([str(h) for h in header])
        csv_buffer.write(h_str + "\n")
        for i, line in enumerate(data):
            current_date = start_date + timedelta(days=i)
            d_str = ",".join([str(d) for d in line])
            csv_buffer.write(current_date.strftime("%Y-%m-%d") + "," + d_str + "\n")
        return csv_buffer

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            if attr:
                data = attr.as_struct(climate_capnp.TimeSeriesData)
            else:
                data = in_ip.content.as_struct(climate_capnp.TimeSeriesData)

            csv = data_to_csv(data.header, data.data, data.startDate)

            out_ip = fbp_capnp.IP.new_message()
            if not config["to_attr"]:
                out_ip.content = csv
            common.copy_and_set_fbp_attrs(in_ip, out_ip, **({config["to_attr"]: csv} if config["to_attr"] else {}))
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": None,
    "from_attr": None,

    "opt:from_attr": "[name:string] -> get sturdy ref or capability from attibute 'from_attr'",
    "opt:to_attr": "[name:string] -> send data attached to attribute 'to_attr'",

    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[climate_capnp.TimeSeriesData]-> ",
    "port:out": "[string (csv)] -> send a timeseries as CSV string",
}
def main():
    parser = c.create_default_fbp_component_args_parser("Write timeseries data to CSV file")
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(parser, default_config)
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))

if __name__ == '__main__':
    main()