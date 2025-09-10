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
import os
from collections import defaultdict
from datetime import date, timedelta

import capnp
from zalfmas_capnp_schemas import climate_capnp, fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


def capnp_date_to_py_date(capnp_date):
    return date(capnp_date.year, capnp_date.month, capnp_date.day)


def aggregate_monthly(header: list, data: list[list[float]], start_date: date):
    grouped_data = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )  # var -> year -> month -> values
    for i, line in enumerate(data):
        current_date = start_date + timedelta(days=i)
        for j, v in enumerate(line):
            grouped_data[header[j]][current_date.year][current_date.month].append(v)

    def aggregate_values(var, values):
        if var == "precip":  # sum precipition
            return sum(values)
        else:  # average all other values
            return sum(values) / len(values)

    vars = {}
    for var, rest1 in grouped_data.items():
        agg_values = []
        for year, rest2 in rest1.items():
            for month, values in rest2.items():
                agg_values.append(int(round(aggregate_values(var, values), 1) * 10))
        vars[var] = ",".join([str(d) for d in agg_values])
    return vars


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=None
    )  # outs taken from infos
    await p.update_config_from_port(config, ports["conf"])

    while ports["in"] and len(list(filter(None, ports.outs))) > 0:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)
            id = common.get_fbp_attr(in_ip, "id")
            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            if attr:
                data = attr.as_struct(climate_capnp.TimeSeriesData)
            else:
                data = in_ip.content.as_struct(climate_capnp.TimeSeriesData)

            vars = aggregate_monthly(
                data.header, data.data, capnp_date_to_py_date(data.startDate)
            )

            for var, out_p in ports.outs.items():
                rs, cs = id.as_text().split("_")
                r = rs.split("-")[1].zfill(3)
                c = cs.split("-")[1].zfill(3)
                line = r + c + "," + vars[var] + "\n"
                out_ip = fbp_capnp.IP.new_message()
                if not config["to_attr"]:
                    out_ip.content = line
                updated_attrs = {"id": var} | (
                    {config["to_attr"]: line} if config["to_attr"] else {}
                )
                common.copy_and_set_fbp_attrs(in_ip, out_ip, **updated_attrs)
                await out_p.write(value=out_ip)

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
    "port:in": "[climate_capnp.TimeSeriesData]-> data to aggregate",
    "port:out": "[string (csv)] -> send a timeseries as CSV string",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Aggregate timeseries data to monthly values and send CSV string"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
