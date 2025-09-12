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
import uuid
from collections import defaultdict

import capnp
from zalfmas_capnp_schemas import common_capnp, fbp_capnp, service_capnp
from zalfmas_common import common
from zalfmas_common import service as serv

import zalfmas_fbp.run.channels as channels


class StopChannelProcess(service_capnp.Stoppable.Server):
    def __init__(self, proc, remove_from_service=None):
        self.proc = proc
        self.remove_from_service = remove_from_service

    async def stop_context(self, context):  # stop @0 () -> (success :Bool);
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            rt = self.proc.returncode == 0
            self.proc = None
            if self.remove_from_service:
                self.remove_from_service()
            context.results.success = rt
        context.results.success = False


class StartChannelsService(fbp_capnp.StartChannelsService.Server, common.Identifiable):
    def __init__(
        self,
        con_man,
        path_to_channel,
        id=None,
        name=None,
        description=None,
        verbose=False,
        admin=None,
        restorer=None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        self.con_man = con_man
        self.path_to_channel = path_to_channel
        self.startup_info_id = str(uuid.uuid4())
        self.channels = {}
        self.first_reader = None
        self.first_writer_sr = None
        self.chan_id_to_info = defaultdict(list)
        self.verbose = verbose

    def __del__(self):
        for _, (chan, _) in self.channels.items():
            chan.terminate()

    async def create_startup_info_channel(self):
        first_chan, first_reader_sr, self.first_writer_sr = (
            channels.start_first_channel(self.path_to_channel)
        )
        self.channels[self.startup_info_id] = (
            first_chan,
            StopChannelProcess(first_chan),
        )
        self.first_reader = await self.con_man.try_connect(
            first_reader_sr, cast_as=fbp_capnp.Channel.Reader
        )

    async def get_start_infos(self, chan, chan_id, no_of_chans):
        if chan_id in self.chan_id_to_info:
            return self.chan_id_to_info.pop(chan_id)
        start_infos = []
        received_infos = 0
        while chan.poll() is None and received_infos < no_of_chans:
            p = (await self.first_reader.read()).value.as_struct(common_capnp.Pair)
            msg_chan_id = p.fst.as_text()
            info = p.snd.as_struct(fbp_capnp.Channel.StartupInfo)
            if chan_id == msg_chan_id:
                received_infos += 1
                start_infos.append(info)
            else:
                self.chan_id_to_info[msg_chan_id].append(info)
        return start_infos

    # struct Params {
    #    name            @0 :Text;       # name of channel
    #    noOfChannels    @1 :UInt16 = 1; # how many channels to create
    #    noOfReaders     @2 :UInt16 = 1; # no of readers to create per channel
    #    noOfWriters     @3 :UInt16 = 1; # no of writers to create per channel
    #    readerSrts      @4 :List(Text); # fixed sturdy ref tokens per reader
    #    writerSrts      @5 :List(Text); # fixed sturdy ref tokens per writer
    #    bufferSize      @6 :UInt16 = 1; # how large is the buffer supposed to be
    # }
    async def start_context(
        self, context
    ):  # start @0 Params -> (startupInfos :List(Channel.StartupInfo), stop :Stoppable);
        if self.first_reader is None:
            await self.create_startup_info_channel()
        ps = context.params
        config_chan_id = str(uuid.uuid4())
        reader_srts = ",".join(ps.readerSrts) if ps._has("readerSrts") else None
        writer_srts = ",".join(ps.writerSrts) if ps._has("writerSrts") else None
        chan = channels.start_channel(
            self.path_to_channel,
            config_chan_id,
            self.first_writer_sr,
            name=ps.name,
            no_of_channels=ps.noOfChannels,
            no_of_readers=ps.noOfReaders,
            no_of_writers=ps.noOfWriters,
            buffer_size=ps.bufferSize,
            reader_srts=reader_srts,
            writer_srts=writer_srts,
            verbose=self.verbose,
        )
        stop = StopChannelProcess(chan, lambda: self.channels.pop(config_chan_id, None))
        self.channels[config_chan_id] = (
            chan,
            StopChannelProcess(chan, lambda: self.channels.pop(config_chan_id, None)),
        )
        context.results.startupInfos = await self.get_start_infos(
            chan, config_chan_id, ps.noOfChannels
        )
        context.results.stop = stop


async def main():
    parser = serv.create_default_args_parser(
        component_description="local start channels service",
        default_config_path="./configs/channel_starter_service.toml",
    )
    config, args = serv.handle_default_service_args(parser, path_to_service_py=__file__)

    restorer = common.Restorer()
    con_man = common.ConnectionManager(restorer)
    service = StartChannelsService(
        con_man,
        config["service"]["path_to_channel"],
        id=config.get("id", None),
        name=config.get("name", None),
        description=config.get("description", None),
        restorer=restorer,
    )
    await service.create_startup_info_channel()
    await serv.init_and_run_service_from_config(
        config=config, service=service, restorer=restorer
    )


if __name__ == "__main__":
    asyncio.run(capnp.run(main()))
