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

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import tomli
from capnp.lib.capnp import (
    KjException,
    _CapabilityClient,
    _DynamicCapabilityClient,
    _InterfaceSchema,
)
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, WriterClient


def get_attr_val(config_val, attrs, as_struct=None, as_interface=None, as_text=False, remove=True):
    if type(config_val) is str and len(config_val) > 0 and config_val[0] == "@" and config_val[1:] in attrs:
        if remove:
            attr_val = attrs.pop(config_val[1:])
        else:
            attr_val = attrs[config_val[1:]]
        if as_struct:
            return attr_val.as_struct(as_struct), True
        elif as_interface:
            return attr_val.as_interface(as_interface), True
        elif as_text:
            return attr_val.as_text(), True
        else:
            return attr_val, True
    else:
        return config_val, False


def get_config_val(config, key, attrs, as_struct=None, as_interface=None, as_text=False, remove=True):
    if key in config:
        cval = config[key]
        return get_attr_val(
            cval,
            attrs,
            as_struct=as_struct,
            as_interface=as_interface,
            as_text=as_text,
            remove=remove,
        )
    else:
        return None, None


# toml or json
async def update_config_from_port(config, port, config_type="toml"):
    if port:
        if xxx_config := await read_dict_from_port(port, config_type):
            config.update(xxx_config)
    return config


async def read_dict_from_port(port, text_type="toml"):
    d = {}
    if port:
        try:
            msg = await port.read()
            if msg.which() == "done":
                return None
            ip = msg.value.as_struct(fbp_capnp.IP)
            try:  # first try to read as structured text
                st = ip.content.as_struct(common_capnp.StructuredText)
                if st.type == "toml":
                    d = tomli.loads(st.value)
                elif st.type == "json":
                    d = json.loads(st.value)
            except (KjException, TypeError, ValueError):
                try:  # if structured text fails, try as plain text and use config_type parameter
                    text_value = ip.content.as_text()
                    if text_type == "toml":
                        d = tomli.loads(text_value)
                    elif text_type == "json":
                        d = json.loads(text_value)
                except (KjException, TypeError, ValueError):
                    pass
        except Exception as e:
            print(f"{os.path.basename(__file__)} read_dict_from_port. Exception: {e}")
    return d


async def read_dict_from_port_done(ports, port_name, text_type="json", set_port_to_none_if_done=True):
    if port_name in ports.ins:
        d = await read_dict_from_port(ports[port_name], text_type=text_type)
        if d is None and set_port_to_none_if_done:
            ports[port_name] = None
        return d
    return None


class PortConnector:
    def __init__(
        self,
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: common.ConnectionManager | None = None,
    ):
        self.in_ports: dict[str, ReaderClient | None] = {n: None for n in ins} if ins else {}
        self.out_ports: dict[str, WriterClient | list[WriterClient | None] | None] = (
            {n: None for n in outs} if outs else {}
        )
        self.con_man: common.ConnectionManager = (
            connection_manager if connection_manager else common.ConnectionManager()
        )
        self.port_infos_reader: ReaderClient | None = None

    # make in_ports a property
    @property
    def ins(self) -> dict[str, ReaderClient | None]:
        return self.in_ports

    # make out_ports a property
    @property
    def outs(self) -> dict[str, WriterClient | list[WriterClient | None] | None]:
        return self.out_ports

    @property
    def connection_manager(self):
        return self.con_man

    def __getitem__(self, key: str) -> ReaderClient | WriterClient | list[WriterClient | None] | dict[str, Any] | None:
        if key in self.in_ports and key in self.out_ports:
            return {
                "in": self.in_ports[key],
                "out": self.out_ports[key],
            }
        if key in self.in_ports:
            return self.in_ports[key]
        if key in self.out_ports:
            return self.out_ports[key]
        return None

    def __setitem__(self, key: str, value) -> None:
        if (
            key in self.in_ports
            and key in self.out_ports
            and isinstance(value, dict)
            and "in" in value
            and "out" in value
        ):
            self.in_ports[key] = value["in"]
            self.out_ports[key] = value["out"]
            return
        if key in self.in_ports:
            self.in_ports[key] = value
            return
        if key in self.out_ports:
            self.out_ports[key] = value

    async def close_out_ports(
        self,
        print_info: bool = False,
        print_exception: bool = True,
        wait_for_port_infos_reader_done: bool = True,
    ):
        for name, ps in self.out_ports.items():
            # is an array out port
            if isinstance(ps, list):
                for i, p in enumerate(ps):
                    if p is not None:
                        try:
                            await p.close()
                            if print_info:
                                print(f"{os.path.basename(__file__)}: closed array out port '{name}[{i}]'")
                        except Exception as e:
                            if print_exception:
                                print(
                                    f"{os.path.basename(__file__)}: Exception closing array out port '{name}[{i}]': {e}"
                                )
            # is a single out port
            elif ps is not None:
                try:
                    await ps.close()
                    if print_info:
                        print(f"{os.path.basename(__file__)}: closed out port '{name}'")
                except Exception as e:
                    if print_exception:
                        print(f"{os.path.basename(__file__)}: Exception closing out port '{name}': {e}")

        if wait_for_port_infos_reader_done:
            if self.port_infos_reader:
                await self.port_infos_reader.read()
            # if msg.which() == "done":
            #  pass

    @staticmethod
    async def create_from_cmd_config(
        config: dict[str, str | None],
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: common.ConnectionManager | None = None,
    ):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_cmd_config(config)
        return pc

    async def connect_from_cmd_config(self, config: dict[str, str | None]):
        try:
            for k, v in config.items():
                if k.endswith("in_sr"):
                    port_name = k[:-6]
                    if len(port_name) == 0:
                        port_name = "in"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.in_ports[port_name] = None
                    else:
                        reader = await self.con_man.try_connect(
                            v,
                            retry_secs=1,
                        )
                        self.in_ports[port_name] = (
                            reader.cast_as(fbp_capnp.Channel.Reader) if reader is not None else None
                        )
                elif k.endswith("out_sr"):
                    port_name = k[:-7]
                    if len(port_name) == 0:
                        port_name = "out"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.out_ports[port_name] = None
                    else:
                        writer = await self.con_man.try_connect(
                            v,
                            retry_secs=1,
                        )
                        self.out_ports[port_name] = (
                            writer.cast_as(fbp_capnp.Channel.Writer) if writer is not None else None
                        )
                elif k.endswith("out_srs"):
                    port_name = k[:-8]
                    if len(port_name) == 0:
                        port_name = "out"
                    elif port_name[:-1] == "_":
                        port_name = port_name[:-1]
                    if v is None:
                        self.out_ports[port_name] = None
                    else:
                        writers: list[WriterClient | None] = []
                        for out_sr in v.split("|"):
                            writer = await self.con_man.try_connect(
                                out_sr,
                                retry_secs=1,
                            )
                            writers.append(writer.cast_as(fbp_capnp.Channel.Writer) if writer is not None else None)
                        self.out_ports[port_name] = writers
        except Exception as e:
            print(
                f"{os.path.basename(__file__)}: Exception connecting to ports via CMD config:\n{config}\n Exception: {e}"
            )

    async def read_or_connect(self, in_port_id: str) -> _DynamicCapabilityClient | _CapabilityClient | None:
        in_port = self.ins[in_port_id]
        if in_port is None:
            return None

        try:
            msg = await in_port.read()
            if msg.which() == "done":
                self.ins[in_port_id] = None
                return None
        except KjException:
            return None

        ip = msg.value.as_struct(fbp_capnp.IP)
        try:
            return ip.content.as_interface(_InterfaceSchema())
        except KjException:
            try:
                return await self.connection_manager.try_connect(
                    ip.content.as_text(),
                    retry_secs=1,
                )
            except Exception as e:
                print(
                    f"{os.path.basename(__file__)}: Error: Couldn't connect to capability from port '{in_port_id}'."
                    f" Exception: {e}"
                )
                return None

    @staticmethod
    async def create_from_port_infos_reader(
        port_infos_reader_sr: str,
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: common.ConnectionManager | None = None,
    ):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_port_infos_reader(port_infos_reader_sr)
        return pc

    async def connect_from_port_infos_reader(self, port_infos_reader_sr: str):
        try:
            port_infos_reader = await self.con_man.try_connect(
                port_infos_reader_sr,
                retry_secs=1,
            )
            self.port_infos_reader = (
                port_infos_reader.cast_as(fbp_capnp.Channel.Reader) if port_infos_reader is not None else None
            )
            if self.port_infos_reader is None:
                return

            pis = (await self.port_infos_reader.read()).value.as_struct(fbp_capnp.PortInfos)
            for n2sr in pis.inPorts:
                if len(n2sr.name) > 0:
                    port_name = n2sr.name
                    if n2sr.which() == "sr" and n2sr.sr is not None:
                        reader = await self.con_man.try_connect(
                            n2sr.sr,
                            retry_secs=1,
                        )
                        self.in_ports[port_name] = (
                            reader.cast_as(fbp_capnp.Channel.Reader) if reader is not None else None
                        )

            for n2sr in pis.outPorts:
                if len(n2sr.name) > 0:
                    port_name = n2sr.name
                    if n2sr.which() == "sr" and n2sr.sr is not None:
                        writer = await self.con_man.try_connect(
                            n2sr.sr,
                            retry_secs=1,
                        )
                        self.out_ports[port_name] = (
                            writer.cast_as(fbp_capnp.Channel.Writer) if writer is not None else None
                        )
                    elif len(n2sr.srs) > 0:
                        writers: list[WriterClient | None] = []
                        for sr in n2sr.srs:
                            if sr is not None:
                                writer = await self.con_man.try_connect(
                                    sr,
                                    retry_secs=1,
                                )
                                writers.append(writer.cast_as(fbp_capnp.Channel.Writer) if writer is not None else None)
                        self.out_ports[port_name] = writers

        except Exception as e:
            print(
                f"{os.path.basename(__file__)}: Exception connecting to ports via port infos reader SR:\n{port_infos_reader_sr}\n Exception: {e}"
            )

    @staticmethod
    async def create_from_toml_str(
        config_toml_str: str,
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: common.ConnectionManager | None = None,
    ):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_toml_str(config_toml_str)
        return pc

    async def connect_from_toml_str(self, config_toml_str: str):
        toml_config = tomli.loads(config_toml_str)

        try:
            for port_name, data in toml_config["ports"]["in"].items():
                sr = data.get("sr", None)
                sr = None if sr == "" else sr
                self.in_ports[port_name] = (
                    reader_cap.cast_as(fbp_capnp.Channel.Reader)
                    if sr and (reader_cap := await self.con_man.try_connect(sr, retry_secs=1)) is not None
                    else None
                )

            for port_name, data in toml_config["ports"]["out"].items():
                if isinstance(data, list):
                    writers: list[WriterClient | None] = []
                    for d in data:
                        sr = d.get("sr", None)
                        sr = None if sr == "" else sr
                        writers.append(
                            writer_cap.cast_as(fbp_capnp.Channel.Writer)
                            if sr and (writer_cap := await self.con_man.try_connect(sr, retry_secs=1)) is not None
                            else None
                        )
                    self.out_ports[port_name] = writers
                else:
                    sr = data.get("sr", None)
                    sr = None if sr == "" else sr
                    self.out_ports[port_name] = (
                        writer_cap.cast_as(fbp_capnp.Channel.Writer)
                        if sr and (writer_cap := await self.con_man.try_connect(sr, retry_secs=1)) is not None
                        else None
                    )

        except Exception as e:
            print(
                f"{os.path.basename(__file__)}: Exception connecting to ports via toml:\n{toml_config}\n Exception: {e}"
            )

    @staticmethod
    async def create_from_toml_reader_sr(
        config_reader_sr: str,
        ins: Sequence[str] | None = None,
        outs: Sequence[str] | None = None,
        connection_manager: common.ConnectionManager | None = None,
    ):
        pc = PortConnector(ins, outs, connection_manager)
        await pc.connect_from_toml_reader_sr(config_reader_sr)
        return pc

    async def connect_from_toml_reader_sr(self, config_reader_sr: str):
        try:
            config_reader_cap = await self.con_man.try_connect(
                config_reader_sr,
                retry_secs=1,
            )
            config_reader = (
                config_reader_cap.cast_as(fbp_capnp.Channel.Reader) if config_reader_cap is not None else None
            )
            if config_reader is None:
                return
            config_msg = await config_reader.read()
            await self.connect_from_toml_str(config_msg.value.as_text())
        except Exception as e:
            print(
                f"{os.path.basename(__file__)}: Exception connecting to config reader via sr ({config_reader_sr}): {e}"
            )
