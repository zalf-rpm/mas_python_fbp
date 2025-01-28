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

#import asyncio
#import capnp
#import os
#from pathlib import Path
import subprocess as sp
import uuid
#from zalfmas_common import common
#from zalfmas_common import fbp
#import zalfmas_capnp_schemas
#sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
#import fbp_capnp

def start_first_channel(path_to_channel, name=None):
    chan = sp.Popen([
        path_to_channel, 
        "--name=chan_{}".format(name if name else str(uuid.uuid4())),
        "--output_srs",
    ], stdout=sp.PIPE, text=True)
    first_reader_sr = None
    first_writer_sr = None
    while True:
        s = chan.stdout.readline().split("=", maxsplit=1)
        id, sr = s if len(s) == 2 else (None, None)
        if id and id == "readerSR":
            first_reader_sr = sr.strip()
        elif id and id == "writerSR":
            first_writer_sr = sr.strip()
        if first_reader_sr and first_writer_sr:
            break
    return chan, first_reader_sr, first_writer_sr


def start_channel(path_to_channel, startup_info_writer_sr, name=None,
                  verbose=False, host=None, port=None, reader_srts=None, writer_srts=None):
    return sp.Popen([path_to_channel,
                     f"--name=chan_{name if name else str(uuid.uuid4())}",
                     f"--startup_info_writer_sr={startup_info_writer_sr}",
                     ] + (["--verbose"] if verbose else [])
                    + ([f"--host={host}"] if host else [])
                    + ([f"--port={port}"] if port else [])
                    + ([f"--reader_srts={reader_srts}"] if reader_srts else [])
                    + ([f"--writer_srts={writer_srts}"] if writer_srts else []))
