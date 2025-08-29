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
import os
import subprocess as sp
import sys
import uuid
from collections import defaultdict

import capnp
import zalfmas_capnp_schemas
from zalfmas_common import common
from zalfmas_common import service as serv

sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import common_capnp
import fbp_capnp
import service_capnp


def start_first_channel(path_to_channel, name=None):
    chan = sp.Popen(
        [
            path_to_channel,
            f"--name=chan_{name if name else str(uuid.uuid4())}",
            "--output_srs",
        ],
        stdout=sp.PIPE,
        text=True,
    )
    first_reader_sr = None
    first_writer_sr = None
    while chan.poll() is None:
        s = chan.stdout.readline().split("=", maxsplit=1)
        id, sr = s if len(s) == 2 else (None, None)
        if id and id.strip() == "readerSR":
            first_reader_sr = sr.strip()
        elif id and id.strip() == "writerSR":
            first_writer_sr = sr.strip()
        if first_reader_sr and first_writer_sr:
            break
    return chan, first_reader_sr, first_writer_sr


def start_channel(
    path_to_channel,
    startup_info_id,
    startup_info_writer_sr,
    name=None,
    verbose=False,
    host=None,
    port=None,
    no_of_channels=1,
    no_of_readers=1,
    no_of_writers=1,
    reader_srts=None,
    writer_srts=None,
    buffer_size=1,
):
    return sp.Popen(
        [
            path_to_channel,
            f"--name=chan_{name if name and len(name) > 0 else str(uuid.uuid4())}",
            f"--startup_info_id={startup_info_id}",
            f"--startup_info_writer_sr={startup_info_writer_sr}",
            f"--no_of_channels={no_of_channels}",
            f"--no_of_readers={no_of_readers}",
            f"--no_of_writers={no_of_writers}",
            f"--buffer_size={buffer_size}",
        ]
        + (["--verbose"] if verbose else [])
        + ([f"--host={host}"] if host else [])
        + ([f"--port={port}"] if port else [])
        + ([f"--reader_srts={reader_srts}"] if reader_srts else [])
        + ([f"--writer_srts={writer_srts}"] if writer_srts else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT
    )


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
        first_chan, first_reader_sr, self.first_writer_sr = start_first_channel(
            self.path_to_channel
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
        chan = start_channel(
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


default_config = {
    "id": str(uuid.uuid4()),
    "name": "local start channels service",
    "description": None,
    "path_to_channel": "path to channel executable here",
    "host": None,
    "port": None,
    "serve_bootstrap": True,
    "fixed_sturdy_ref_token": None,
    "reg_sturdy_ref": None,
    "reg_category": None,
    "verbose": False,
    "opt:id": "ID of the service",
    "opt:name": "local FBP components service",
    "opt:description": "Serves locally startable components",
    "opt:path_to_channel": "[string (path)] -> Path to JSON containing the mapping of component id to commandline execution",
    "opt:host": "[string (IP/hostname)] -> Use this host (e.g. localhost)",
    "opt:port": "[int] -> Use this port (missing = default = choose random free port)",
    "opt:serve_bootstrap": "[true | false] -> Is the service reachable directly via its restorer interface",
    "opt:fixed_sturdy_ref_token": "[string] -> Use this token as the sturdy ref token of this service",
    "opt:reg_sturdy_ref": "[string (sturdy ref)] -> Connect to registry using this sturdy ref",
    "opt:reg_category": "[string] -> Connect to registry using this category",
}


async def main():
    parser = serv.create_default_args_parser(
        "local start channels service", "./configs/channel_starter_service.toml"
    )
    config, args = serv.handle_default_service_args(parser, default_config)

    restorer = common.Restorer()
    con_man = common.ConnectionManager(restorer)
    service = StartChannelsService(
        con_man,
        config["path_to_channel"],
        id=config["id"],
        name=config["name"],
        description=config["description"],
        restorer=restorer,
    )
    await service.create_startup_info_channel()
    await serv.init_and_run_service(
        {"service": service},
        config["host"],
        config["port"],
        serve_bootstrap=config["serve_bootstrap"],
        name_to_service_srs={"service": config["fixed_sturdy_ref_token"]},
        restorer=restorer,
    )


if __name__ == "__main__":
    asyncio.run(capnp.run(main()))


# class Channel(fbp_capnp.Channel.Server, common.Identifiable, common.Persistable, serv.AdministrableService):
#
#     def __init__(self, buffer_size=1, auto_close_semantics="fbp",
#                  id=None, name=None, description=None, admin=None, restorer=None):
#         common.Identifiable.__init__(self, id, name, description)
#         common.Persistable.__init__(self, restorer)
#         serv.AdministrableService.__init__(self, admin)
#
#         self._id = str(id if id else uuid.uuid4())
#         self._name = name if name else self._id
#         self._description = description if description else ""
#         self._buffer = deque()
#         self._buffer_size = buffer_size
#         self._readers = []
#         self._writers = []
#         self._blocking_read_fulfillers = deque()
#         self._blocking_write_fulfillers = deque()
#         self._auto_close_semantics = auto_close_semantics
#
#     def shutdown(self):
#         print("Channel::shutdown")
#         # capnp.reset_event_loop()
#         exit(0)
#
#     def closed_reader(self, reader):
#         print("Channel::closed_reader")
#         self._readers.remove(reader)
#         # if self._auto_close_semantics == "fbp" and len(self._readers) == 0:
#         # capnp.getTimer().after_delay(1*10**9).then(lambda: self.shutdown())
#         # Timer(1, self.shutdown).start()
#
#     def closed_writer(self, writer):
#         print("Channel::closed_writer")
#         self._writers.remove(writer)
#         if self._auto_close_semantics == "fbp" and len(self._writers) == 0:
#             for r in self._readers:
#                 r.send_close_on_empty_buffer = True
#             # as we just received a done message which should be distributed and would
#             # fill the buffer, unblock all readers, so they send the done message
#             while len(self._blocking_read_fulfillers) > 0:
#                 self._blocking_read_fulfillers.popleft().fulfill()
#
#     def setBufferSize_context(self, context):  # setBufferSize @0 (size :UInt64);
#         self._buffer_size = max(1, context.params.size)
#
#     def reader_context(self, context):  # reader @1 () -> (r :Reader);
#         r = self.create_reader()
#         context.results.r = r
#
#     def writer_context(self, context):  # writer @2 () -> (w :Writer);
#         w = self.create_writer()
#         context.results.w = w
#
#     def create_reader(self):
#         r = Reader(self)
#         self._readers.append(r)
#         return r
#
#     def create_writer(self):
#         w = Writer(self)
#         self._writers.append(w)
#         return w
#
#     def create_reader_writer_pair(self):
#         r = self.create_reader()
#         w = self.create_writer()
#         return {"r": r, "w": w}
#
#     def endpoints_context(self, context):  # endpoints @1 () -> (r :Reader, w :Writer);
#         ep = self.create_reader_writer_pair()
#         context.results.r = ep["r"]
#         context.results.w = ep["w"]
#
#     def setAutoCloseSemantics_context(self, context):  # setAutoCloseSemantics @4 (cs :CloseSemantics);
#         self._auto_close_semantics = context.params.cs
#
#     def close_context(self, context):  # close @5 (waitForEmptyBuffer :Bool = true);
#         # make new writes refused
#         for w in self._writers:
#             w.closed = True
#         # forget writers
#         self._writers.clear()
#
#         if context.params.waitForEmptyBuffer:
#             # cancel blocking writers
#             for paf in self._blocking_write_fulfillers:
#                 paf.promise.cancel()
#             for r in self._readers:
#                 r.send_close_on_empty_buffer = True
#             while len(self._blocking_read_fulfillers) > 0:
#                 self._blocking_read_fulfillers.popleft().fulfill()
#         else:
#             # close all readers
#             for r in self._readers:
#                 r.closed = True
#             # and forget them
#             self._readers.clear()
#             # return capnp.getTimer().after_delay(1*10**9).then(lambda: self.shutdown())
#             self.shutdown()
#
#
# class Reader(fbp_capnp.Channel.Reader.Server):
#
#     def __init__(self, channel):
#         self._channel = channel
#         self._send_close_on_empty_buffer = False
#         self._closed = False
#
#     @property
#     def closed(self):
#         return self._closed
#
#     @closed.setter
#     def closed(self, v):
#         self._closed = v
#
#     @property
#     def send_close_on_empty_buffer(self):
#         return self._send_close_on_empty_buffer
#
#     @send_close_on_empty_buffer.setter
#     def send_close_on_empty_buffer(self, v):
#         self._send_close_on_empty_buffer = v
#
#     def read_context(self, context):  # read @0 () -> (value :V);
#         if self.closed:
#             raise Exception("Reader closed")
#
#         c = self._channel
#         b = c._buffer
#
#         def set_results_from_buffer():
#             if self.closed:
#                 raise Exception("Reader closed")
#
#             if self.send_close_on_empty_buffer and len(b) == 0:
#                 context.results.done = None
#                 c.closed_reader(self)
#             elif len(b) > 0:
#                 context.results.value = fbp_capnp.Channel.Msg.from_bytes(b.popleft()).value
#
#         # read value non-blocking
#         if len(b) > 0:
#             set_results_from_buffer()
#             # print(c.name, "r ", sep="", end="")
#
#             # unblock writers unless we're about to close down
#             if len(b) == 0 and not self.send_close_on_empty_buffer:
#                 # unblock potentially waiting writers
#                 while len(b) < c._buffer_size and len(c._blocking_write_fulfillers) > 0:
#                     c._blocking_write_fulfillers.popleft().fulfill()
#
#         else:  # block because no value to read
#             # if the channel is supposed to close down, just generate a close message
#             if self.send_close_on_empty_buffer:
#                 print("Reader::read_context -> len(b) == 0 -> send done")
#                 set_results_from_buffer()
#
#                 # as the buffer is empty and we're supposed to shut down as any other reader
#                 # fulfill waiting readers with done messages
#                 while len(c._blocking_read_fulfillers) > 0:
#                     c._blocking_read_fulfillers.popleft().fulfill()
#             else:
#                 paf = capnp.PromiseFulfillerPair()
#                 c._blocking_read_fulfillers.append(paf)
#
#                 # print("[", c.name, "r"+str(len(c._blocking_read_fulfillers))+"] ", sep="", end="")
#                 return paf.promise.then(set_results_from_buffer)
#
#     def close_context(self, context):  # close @1 ();
#         if self.closed:
#             raise Exception("Reader closed")
#
#         self._channel.closed_reader(self)
#         self.closed = True
#
#
# class Writer(fbp_capnp.Channel.Writer.Server):
#
#     def __init__(self, channel):
#         self._channel = channel
#         self._closed = False
#
#     @property
#     def closed(self):
#         return self._closed
#
#     @closed.setter
#     def closed(self, v):
#         self._closed = v
#
#     def write_context(self, context):  # write @0 (value :V);
#         if self.closed:
#             raise Exception("Writer closed")
#
#         v = context.params
#         c = self._channel
#         b = c._buffer
#
#         def append_from_buffer():
#             if self.closed:
#                 raise Exception("Writer closed")
#             b.append(v.as_builder().to_bytes())
#
#         # if we received a done, this writer can be removed
#         if v.which() == "done":
#             c.closed_writer(self)
#             return
#
#         # write value non-block
#         if len(b) < c._buffer_size:
#             append_from_buffer()
#             # print(c.name, "w ", sep="", end="")
#             # unblock potentially waiting readers
#             while len(b) > 0 and len(c._blocking_read_fulfillers) > 0:
#                 c._blocking_read_fulfillers.popleft().fulfill()
#         else:  # block until buffer has space
#             paf = capnp.PromiseFulfillerPair()
#             c._blocking_write_fulfillers.append(paf)
#             # print("[", c.name, "w"+str(len(c._blocking_write_fulfillers))+"] ", sep="", end="")
#             return paf.promise.then(append_from_buffer)
#
#     def close_context(self, context):  # close @1 ();
#         if self.closed:
#             raise Exception("Writer closed")
#
#         self._channel.closed_writer(self)
#         self.closed = True
#
#
# async def main(no_of_channels=1, buffer_size=1, serve_bootstrap=True, host=None, port=None,
#                id=None, name="Channel Service", description=None, use_async=False):
#     config = {
#         "no_of_channels": str(no_of_channels),
#         "buffer_size": str(max(1, buffer_size)),
#         "reader_srts": "[[r1_1234,r2_1234]]",  # [[c1_r1,c1_r2,c1_r3],[c1_r1],...]
#         "writer_srts": "[[w1_1234]]",  # [[c1_w1,c1_w2],[c2_w1],...]
#         "port": port,
#         "host": host,
#         "id": id,
#         "name": name,
#         "description": description,
#         "serve_bootstrap": serve_bootstrap,
#         "use_async": use_async,
#         "store_srs_file": None
#     }
#
#     common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)
#
#     channel_to_reader_srts = defaultdict(list)
#     channel_to_writer_srts = defaultdict(list)
#     if "reader_srts" in config:
#         for i, srts in enumerate(json.loads(config["reader_srts"])):
#             channel_to_reader_srts[i + 1] = srts
#     if "writer_srts" in config:
#         for i, srts in enumerate(json.loads(config["writer_srts"])):
#             channel_to_writer_srts[i + 1] = srts
#
#     restorer = common.Restorer()
#     services = {}
#     name_to_service_srs = {}
#     for i in range(1, int(config["no_of_channels"]) + 1):
#         c = Channel(buffer_size=int(config["buffer_size"]),
#                     id=config["id"], name=str(i), description=config["description"], restorer=restorer)
#         services["channel_" + str(i)] = c
#         for k, srt in enumerate(channel_to_reader_srts.get(i, [None])):
#             reader_name = "channel_" + str(i) + "_reader_" + str(k + 1)
#             name_to_service_srs[reader_name] = srt
#             services[reader_name] = c.create_reader()
#         for k, srt in enumerate(channel_to_writer_srts.get(i, [None])):
#             writer_name = "channel_" + str(i) + "_writer_" + str(k + 1)
#             name_to_service_srs[writer_name] = srt
#             services[writer_name] = c.create_writer()
#
#     store_srs_file_path = config["store_srs_file"]
#
#     def write_srs():
#         if store_srs_file_path:
#             with open(store_srs_file_path, mode="wt") as _:
#                 _.write(json.dumps(name_to_service_srs))
#
#     await serv.async_init_and_run_service(services, config["host"], config["port"],
#                                           serve_bootstrap=config["serve_bootstrap"], restorer=restorer,
#                                           name_to_service_srs=name_to_service_srs,
#                                           run_before_enter_eventloop=write_srs)
#
#
# if __name__ == '__main__':
#     asyncio.run(main(no_of_channels=1, buffer_size=1, serve_bootstrap=True))
