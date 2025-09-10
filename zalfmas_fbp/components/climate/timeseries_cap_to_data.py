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

import capnp
from zalfmas_capnp_schemas import climate_capnp, fbp_capnp
from zalfmas_common import common

import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    ports = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr, ins=["conf", "in"], outs=["out"]
    )
    await p.update_config_from_port(config, ports["conf"])

    def create_capnp_date(py_date):  # isodate):
        # py_date = date.fromisoformat(isodate)
        return {"year": py_date.year, "month": py_date.month, "day": py_date.day}

    def set_capnp_date(capnp_date, py_date):  # isodate):
        # py_date = date.fromisoformat(isodate)
        capnp_date.year = py_date.year
        capnp_date.month = py_date.month
        capnp_date.day = py_date.day

    while ports["in"] and ports["out"]:
        try:
            in_msg = await ports["in"].read()
            if in_msg.which() == "done":
                ports["in"] = None
                continue

            in_ip = in_msg.value.as_struct(fbp_capnp.IP)

            # pass through brackets as we just want to preserve structure for downstream components
            if in_ip.type == "openBracket":
                if config["maintain_substreams"]:
                    await ports["out"].write(in_ip)
                continue
            elif in_ip.type == "closeBracket":
                if config["maintain_substreams"]:
                    await ports["out"].write(in_ip)

            attr = common.get_fbp_attr(in_ip, config["from_attr"])
            cap_or_sr = attr if attr else in_ip.content
            timeseries = None
            try:
                timeseries = cap_or_sr.as_interface(climate_capnp.TimeSeries)
            except Exception:
                try:
                    timeseries = await ports.connection_manager.try_connect(
                        cap_or_sr.as_text(),
                        cast_as=climate_capnp.TimeSeries,
                        retry_secs=1,
                    )
                except Exception as e:
                    print(
                        "Error: Couldn't connect to timeseries.",
                        cap_or_sr,
                        "Exception:",
                        e,
                    )
                    continue
            if timeseries is None:
                continue

            tsd = climate_capnp.TimeSeriesData.new_message()
            tsd.isTransposed = config["transposed"] == "true"

            if config["subheader"]:
                subheader = config["subheader"].split(",")
                timeseries = timeseries.subheader(subheader).timeSeries
            header = (await timeseries.header()).header

            if config["subrange_start"] or config["subrange_to"]:
                # sr_req = timeseries.subrange_request()
                # timeseries = timeseries.subrange(
                #    ({"from": create_capnp_date(config["subrange_start"])} if config["subrange_start"] else {}),
                #    ({"to": create_capnp_date(config["subrange_end"])} if config["subrange_end"] else {})).timeSeries
                timeseries = timeseries.subrange(
                    create_capnp_date(config["subrange_start"]),
                    create_capnp_date(config["subrange_end"]),
                ).timeSeries
                # if config["subrange_start"]:
                #    set_capnp_date(sr_req.start, config["subrange_start"])
                # if config["subrange_end"]:
                #    set_capnp_date(sr_req.end, config["subrange_end"])
                # timeseries = (await sr_req.send()).timeSeries

            # resolution_prom = timeseries.resolution()
            resolution = timeseries.resolution()
            se_date_prom = timeseries.range()
            header_size = len(header)
            ds = (
                (await timeseries.dataT()).data
                if tsd.isTransposed
                else (await timeseries.data()).data
            )
            tsd.init("data", len(ds))
            for i in range(len(ds)):
                l = tsd.data.init(i, header_size)
                for j in range(header_size):
                    l[j] = ds[i][j]
            se_date = await se_date_prom
            tsd.startDate = se_date.startDate
            tsd.endDate = se_date.endDate
            tsd.resolution = (await resolution).resolution
            # tsd.resolution = resolution_prom.resolution
            h = tsd.init("header", len(header))
            for i in range(len(header)):
                h[i] = header[i]

            out_ip = fbp_capnp.IP.new_message()
            if not config["to_attr"]:
                out_ip.content = tsd
            common.copy_and_set_fbp_attrs(
                in_ip, out_ip, **({config["to_attr"]: tsd} if config["to_attr"] else {})
            )
            await ports["out"].write(value=out_ip)

        except Exception as e:
            print(f"{os.path.basename(__file__)} Exception:", e)

    await ports.close_out_ports()
    print(f"{os.path.basename(__file__)}: process finished")


default_config = {
    "to_attr": None,
    "from_attr": None,
    "subrange_start": None,
    "subrange_end": None,
    "subheader": None,
    "transposed": "false",
    "maintain_substreams": False,
    "opt:from_attr": "[name:string] -> get sturdy ref or capability from attibute 'from_attr'",
    "opt:to_attr": "[name:string] -> send data attached to attribute 'to_attr'",
    "opt:subrange_start": "[None | iso-date string] -> start timeseries at that day",
    "opt:subrange_end": "[None | iso-date string] -> end timeseries at that day",
    "opt:subheader": "[None | list[string] (precip,globrad,tavg,...)] -> select all (None) or just listed climate elements in dataset",
    "opt:transposed": "[true | false] -> get data transposed (timeseries of each climate element instead of timeseries of all climate elements each day",
    "opt:maintain_substreams": "[true | false] -> if false, ignore bracket IPs and thus flatten substreams",
    "port:conf": "[TOML string] -> component configuration",
    "port:in": "[string (sturdy ref) to climate.capnp:TimeSeries | climate.capnp:TimeSeries] -> ",
    "port:out": "[climate.capnp:TimeSeriesData] -> send a timeseries as TimeSeriesData struct",
}


def main():
    parser = c.create_default_fbp_component_args_parser(
        "Retrieve timeseries data from capability and forward data downstream"
    )
    port_infos_reader_sr, config, args = c.handle_default_fpb_component_args(
        parser, default_config
    )
    asyncio.run(capnp.run(run_component(port_infos_reader_sr, config)))


if __name__ == "__main__":
    main()
