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
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess as sp
import sys
from collections.abc import Callable, Iterable
from contextlib import suppress
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, cast, overload, override

import capnp
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.fbp.fbp_capnp.types.results.tuples import (
    ConnectinportResultTuple,
    ConnectoutportResultTuple,
)

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, StateTransitionClient, WriterClient
    from mas.schema.fbp.fbp_capnp.types.enums import ProcessStateEnum
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader
    from mas.schema.fbp.fbp_capnp.types.results.client import ReadResult
from zalfmas_common import common

from zalfmas_fbp.run.logging_config import add_log_level_argument, configure_logging

ArrayReaderPorts = list["ReaderClient | None"]
ArrayWriterPorts = list["WriterClient | None"]
DEFAULT_SOFT_STOP_TIMEOUT_SECONDS = 30.0

logger = logging.getLogger(__name__)
configure_logging()


class ArrayInStrategy(StrEnum):
    ZIP = "zip"
    NEXT_AVAILABLE = "next_available"


class ArrayOutStrategy(StrEnum):
    BROADCAST = "broadcast"
    ROUND_ROBIN = "round_robin"


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ProcessStateEnum, ProcessStateEnum],
            None,
        ],  #: Callable[[fbp_capnp.Process.State, fbp_capnp.Process.State]]
    ):
        self.callback: Callable[[ProcessStateEnum, ProcessStateEnum], None] = callback

    # stateChanged @0 (old :State, new :State);
    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class PortDisconnect(fbp_capnp.Process.Disconnect.Server):
    def __init__(
        self,
        ports: dict[str, Any] | dict[str, list[Any]],
        name: str,
        port: Any,
        *,
        index: int | None = None,
    ):
        self.ports = ports
        self.name = name
        self.port = port
        self.index = index
        self.disconnected = False

    async def disconnect(self, _context=None, **_kwargs):
        if self.disconnected or self.port is None:
            return False

        if self.index is None:
            disconnected = self._disconnect_standard_port()
        else:
            disconnected = self._disconnect_array_port()

        if disconnected:
            await self._close_port()
            self.disconnected = True
        return disconnected

    def _disconnect_standard_port(self) -> bool:
        ports = cast("dict[str, Any]", self.ports)
        if ports.get(self.name) is not self.port:
            return False
        ports[self.name] = None
        return True

    def _disconnect_array_port(self) -> bool:
        ports = cast("dict[str, list[Any]]", self.ports)
        array_ports = ports.get(self.name)
        if array_ports is None:
            return False

        if self.index is not None and self.index < len(array_ports) and array_ports[self.index] is self.port:
            array_ports[self.index] = None
            return True

        for i, port in enumerate(array_ports):
            if port is self.port:
                array_ports[i] = None
                return True
        return False

    async def _close_port(self) -> None:
        if self.port is None:
            return
        close = getattr(self.port, "close", None)
        if close is None:
            return
        try:
            await close()
        except (capnp.KjException, RuntimeError) as e:
            logger.error("Exception closing disconnected port '%s': %s", self.name, e)


class Process(fbp_capnp.Process.Server, common.Identifiable, common.GatewayRegistrable):
    def __init__(
        self,
        metadata: dict[str, Any] | None = None,
        con_man: common.ConnectionManager | None = None,
        id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ):
        common.Identifiable.__init__(self, id=id, name=name, description=description)
        common.GatewayRegistrable.__init__(self, con_man or common.ConnectionManager())

        self.metadata: dict[str, Any] = metadata or {}
        self.configuration: dict[str, Any] = {}
        self.in_ports: dict[str, ReaderClient | None] = {}
        self.array_in_ports: dict[str, ArrayReaderPorts] = {}
        self._array_in_buffers: dict[str, dict[int, IPReader]] = {}
        self.out_ports: dict[str, WriterClient | None] = {}
        self.array_out_ports: dict[str, ArrayWriterPorts] = {}
        self._array_out_next_indices: dict[str, int] = {}
        self.tasks = []
        self._run_task: asyncio.Task[None] | None = None
        self._run_exception: BaseException | None = None
        self._stop_requested: asyncio.Event = asyncio.Event()
        self.soft_stop_timeout_seconds = DEFAULT_SOFT_STOP_TIMEOUT_SECONDS
        self.process_state: ProcessStateEnum = "idle"
        self.state_transition_callbacks: list[StateTransitionClient] = []

        self.init_from_metadata()

    @staticmethod
    def _is_array_port(port_info: dict[str, Any]) -> bool:
        return port_info.get("type") == "array"

    @staticmethod
    def _port_message(name: str, port_info: dict[str, Any] | None, port_type: str):
        port_info = port_info or {}
        return {
            "name": name,
            "type": port_type,
            "contentType": port_info.get("contentType", "Text"),
        }

    def init_from_metadata(self):
        default_config: dict[str, Any] = {}
        if self.meta:
            try:
                component_meta = self.meta["component"]
                default_config = {k: v["value"] for k, v in component_meta.get("defaultConfig", {}).items()}
                self.name: str = component_meta["info"]["name"]
                self.description: str = component_meta["info"]["description"]
                for port_info in component_meta.get("inPorts", []):
                    name = port_info["name"]
                    if self._is_array_port(port_info):
                        self.array_in_ports.setdefault(name, [])
                        self._array_in_buffers.setdefault(name, {})
                    else:
                        self.in_ports.setdefault(name, None)
                for port_info in component_meta.get("outPorts", []):
                    name = port_info["name"]
                    if self._is_array_port(port_info):
                        self.array_out_ports.setdefault(name, [])
                        self._array_out_next_indices.setdefault(name, 0)
                    else:
                        self.out_ports.setdefault(name, None)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(
                    "Some metadata could not be used for initializing the process component. Exception: %s",
                    e,
                )
        for k, v in default_config.items():
            val = None
            vt = type(v)
            if vt is str:
                val = common_capnp.Value.new_message(t=v)
            elif vt is int:
                val = common_capnp.Value.new_message(i64=v)
            elif vt is float:
                val = common_capnp.Value.new_message(f64=v)
            elif vt is bool:
                val = common_capnp.Value.new_message(b=v)
            elif vt is dict:
                val = common_capnp.Value.new_message(t=json.dumps(v))
            elif vt is list and len(v) > 0 and type(v[0]) is int:
                if all(map(lambda x: type(x) is int, v)):
                    val = common_capnp.Value.new_message(li64=v)
            elif vt is list and len(v) > 0 and type(v[0]) is float:
                if all(map(lambda x: type(x) is float, v)):
                    val = common_capnp.Value.new_message(lf64=v)
            elif vt is list and len(v) > 0 and type(v[0]) is bool:
                if all(map(lambda x: type(x) is bool, v)):
                    val = common_capnp.Value.new_message(lb=v)
            elif vt is list and len(v) > 0 and type(v[0]) is str:
                if all(map(lambda x: type(x) is str, v)):
                    val = common_capnp.Value.new_message(lt=v)

            if val:
                self.config[k] = val

    @property
    def meta(self):
        return self.metadata

    @property
    def config(self):
        return self.configuration

    # inPorts @0 () -> (ports :List(Component.Port));
    async def inPorts(self, _context, **kwargs):
        component_meta = self.meta.get("component", {})
        in_port_infos = {p["name"]: p for p in component_meta.get("inPorts", [])}
        ports = [self._port_message(k, in_port_infos.get(k), "standard") for k in self.in_ports]
        ports.extend(self._port_message(k, in_port_infos.get(k), "array") for k in self.array_in_ports)
        return ports

    # connectInPort @1 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectInPort(self, name: str, sturdyRef, _context, **kwargs) -> ConnectinportResultTuple:
        reader = (
            reader_cap.cast_as(fbp_capnp.Channel.Reader)
            if (reader_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        if name in self.array_in_ports:
            index = len(self.array_in_ports[name])
            self.array_in_ports[name].append(reader)
            self._array_in_buffers.setdefault(name, {})
            return ConnectinportResultTuple(
                reader is not None,
                PortDisconnect(self.array_in_ports, name, reader, index=index),
            )

        self.in_ports[name] = reader
        return ConnectinportResultTuple(
            self.in_ports[name] is not None,
            PortDisconnect(self.in_ports, name, reader),
        )

    # outPorts @2 () -> (ports :List(Component.Port));
    async def outPorts(self, _context, **kwargs):
        component_meta = self.meta.get("component", {})
        out_port_infos = {p["name"]: p for p in component_meta.get("outPorts", [])}
        ports = [self._port_message(k, out_port_infos.get(k), "standard") for k in self.out_ports]
        ports.extend(self._port_message(k, out_port_infos.get(k), "array") for k in self.array_out_ports)
        return ports

    # connectOutPort @3 (name :Text, sturdyRef :SturdyRef) -> (connected :Bool);
    async def connectOutPort(self, name: str, sturdyRef, _context, **kwargs) -> ConnectoutportResultTuple:
        writer = (
            writer_cap.cast_as(fbp_capnp.Channel.Writer)
            if (writer_cap := await self.con_man.try_connect(sturdyRef)) is not None
            else None
        )
        if name in self.array_out_ports:
            index = len(self.array_out_ports[name])
            self.array_out_ports[name].append(writer)
            self._array_out_next_indices.setdefault(name, 0)
            return ConnectoutportResultTuple(
                writer is not None,
                PortDisconnect(self.array_out_ports, name, writer, index=index),
            )

        self.out_ports[name] = writer
        return ConnectoutportResultTuple(
            self.out_ports[name] is not None,
            PortDisconnect(self.out_ports, name, writer),
        )

    # configEntries @4 () -> (config :List(ConfigEntry));
    async def configEntries(self, _context, **kwargs):
        return list(
            map(
                lambda item: fbp_capnp.Process.ConfigEntry.new_message(name=item[0], val=item[1]),
                self.config.items(),
            ),
        )

    # struct ConfigEntry {
    #     name @0 :Text;
    #     val  @1 :Common.Value;
    # }
    # setConfigEntry @7 ConfigEntry;
    async def setConfigEntry_context(self, context):
        ps = context.params
        self.config[ps.name] = ps.val.as_builder()
        # print(f"received config entry: {ps.name} with value: {ps.val}")

    async def transition_to_state(self, new_state: ProcessStateEnum):
        if new_state == self.process_state:
            return

        prev_state = self.process_state
        self.process_state = new_state
        for cb in self.state_transition_callbacks:
            await cb.stateChanged(prev_state, self.process_state)

    # start @5 ();
    @override
    async def start(self, _context=None, **kwargs) -> bool:
        if self._run_task is not None and not self._run_task.done():
            return False
        if self.process_state in ("starting", "running", "stopping", "closed"):
            return False

        self._stop_requested = asyncio.Event()
        self._run_exception = None
        self._run_task = asyncio.create_task(self._run_wrapper(), name=f"{self.name or self.id}-run")
        return True

    async def _run_wrapper(self):
        final_state: ProcessStateEnum = "idle"
        try:
            if self._stop_requested.is_set():
                return
            await self.transition_to_state("starting")
            if self._stop_requested.is_set():
                return

            await self.transition_to_state("running")
            await self.run()
        except asyncio.CancelledError:
            if not self._stop_requested.is_set():
                final_state = "failed"
                self._run_exception = sys.exception()
                logger.exception("%s run task was cancelled unexpectedly", self.name)
                raise
        except Exception:
            final_state = "failed"
            self._run_exception = sys.exception()
            logger.exception("%s process failed", self.name)
        finally:
            await self.close_out_ports()
            if self.process_state != "closed":
                await self.transition_to_state(final_state)
            self._stop_requested.clear()

    # stop @6 () -> (stopped :Bool);
    @override
    async def stop(self, _context=None, **kwargs) -> bool:
        has_running_task = self._run_task is not None and not self._run_task.done()
        if not has_running_task and self.process_state in ("idle", "failed", "closed"):
            return False

        self._stop_requested.set()
        await self.transition_to_state("stopping")
        return await self._wait_for_run_task(self.soft_stop_timeout_seconds)

    async def _wait_for_run_task(self, timeout: float) -> bool:
        if self._run_task is None:
            return True
        if self._run_task.done():
            await self._consume_run_task_result()
            return True

        done, _pending = await asyncio.wait({self._run_task}, timeout=timeout)
        if self._run_task not in done:
            logger.error(
                "%s run task did not stop within %.3fs",
                self.name,
                timeout,
            )
            return False

        await self._consume_run_task_result()
        return True

    async def _consume_run_task_result(self) -> None:
        if self._run_task is None:
            return
        try:
            await self._run_task
        except asyncio.CancelledError:
            pass

    async def run(self):
        logger.warning("run method unimplemented")

    # state @8 (transitionCallback :StateTransition) -> (currentState :State);
    @override
    async def state(self, transitionCallback, _context, **kwargs):
        if transitionCallback:
            self.state_transition_callbacks.append(transitionCallback)
        return self.process_state

    @property
    def stopping(self) -> bool:
        return self._stop_requested.is_set()

    async def read_in(self, name: str) -> IPReader | None:
        if self._stop_requested.is_set():
            return None

        port = self.in_ports.get(name)
        if port is None:
            return None

        read_task = asyncio.ensure_future(port.read())
        stop_task = asyncio.create_task(self._stop_requested.wait())
        try:
            done, pending = await asyncio.wait(
                {read_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

            if stop_task in done:
                read_task.cancel()
                with suppress(asyncio.CancelledError):
                    await read_task
                return None

            msg = await read_task
            if msg.which() == "done":
                self.in_ports[name] = None
                return None
            return msg.value.as_struct(fbp_capnp.IP)
        except asyncio.CancelledError:
            read_task.cancel()
            stop_task.cancel()
            with suppress(asyncio.CancelledError):
                await read_task
            raise
        except capnp.KjException as e:
            logger.error("%s RPC exception reading input port '%s': %s", self.name, name, getattr(e, "description", e))
            self.in_ports[name] = None
            return None
        finally:
            if not stop_task.done():
                stop_task.cancel()

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.ZIP, "zip"] = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | None: ...

    @overload
    async def read_array_in(
        self,
        name: str,
        strategy: Literal[ArrayInStrategy.NEXT_AVAILABLE, "next_available"],
    ) -> IPReader | None: ...

    async def read_array_in(
        self,
        name: str,
        strategy: ArrayInStrategy | str = ArrayInStrategy.ZIP,
    ) -> list[IPReader] | IPReader | None:
        strategy = ArrayInStrategy(strategy)
        if self._stop_requested.is_set():
            return None

        ports = self.array_in_ports.get(name)
        if not ports:
            return None

        active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
        if not active_ports:
            return None

        if strategy == ArrayInStrategy.NEXT_AVAILABLE:
            return await self._read_array_in_next_available(name, active_ports, ports)

        if strategy != ArrayInStrategy.ZIP:
            raise ValueError(f"Unsupported array input strategy: {strategy}")

        read_tasks: dict[asyncio.Future[ReadResult], int] = {
            asyncio.ensure_future(port.read()): i for i, port in active_ports
        }
        stop_task = asyncio.create_task(self._stop_requested.wait())
        results: dict[int, IPReader] = {}
        zip_finished = False

        try:
            while read_tasks:
                done, _pending = await asyncio.wait(
                    {*read_tasks, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_task in done:
                    await self._cancel_tasks(read_tasks)
                    return None

                for task in done:
                    if task is stop_task:
                        continue

                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        logger.error(
                            "%s RPC exception reading array input port '%s[%s]': %s",
                            self.name,
                            name,
                            port_index,
                            getattr(e, "description", e),
                        )
                        ports[port_index] = None
                        zip_finished = True
                        continue

                    if msg.which() == "done":
                        ports[port_index] = None
                        zip_finished = True
                    else:
                        results[port_index] = msg.value.as_struct(fbp_capnp.IP)

                if zip_finished:
                    await self._cancel_tasks(read_tasks)
                    return None

            return [results[i] for i, _port in active_ports]
        except asyncio.CancelledError:
            await self._cancel_tasks(read_tasks)
            stop_task.cancel()
            raise
        finally:
            if not stop_task.done():
                stop_task.cancel()

    async def _read_array_in_next_available(
        self,
        name: str,
        active_ports: list[tuple[int, ReaderClient]],
        ports: ArrayReaderPorts,
    ) -> IPReader | None:
        buffers = self._array_in_buffers.setdefault(name, {})
        if buffers:
            port_index = next(iter(buffers))
            return buffers.pop(port_index)

        while active_ports:
            read_tasks: dict[asyncio.Future[ReadResult], int] = {
                asyncio.ensure_future(port.read()): i for i, port in active_ports
            }
            stop_task = asyncio.create_task(self._stop_requested.wait())

            try:
                done, _pending = await asyncio.wait(
                    {*read_tasks, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if stop_task in done:
                    await self._cancel_tasks(read_tasks)
                    return None

                for task in done:
                    if task is stop_task:
                        continue

                    read_task = cast("asyncio.Future[ReadResult]", task)
                    port_index = read_tasks.pop(read_task)
                    try:
                        msg = await read_task
                    except capnp.KjException as e:
                        logger.error(
                            "%s RPC exception reading array input port '%s[%s]': %s",
                            self.name,
                            name,
                            port_index,
                            getattr(e, "description", e),
                        )
                        ports[port_index] = None
                        continue

                    if msg.which() == "done":
                        ports[port_index] = None
                    else:
                        buffers[port_index] = msg.value.as_struct(fbp_capnp.IP)

                await self._cancel_tasks(read_tasks)
                if buffers:
                    port_index = next(iter(buffers))
                    return buffers.pop(port_index)

                active_ports = [(i, port) for i, port in enumerate(ports) if port is not None]
            except asyncio.CancelledError:
                await self._cancel_tasks(read_tasks)
                stop_task.cancel()
                raise
            finally:
                if not stop_task.done():
                    stop_task.cancel()

        return None

    @staticmethod
    async def _cancel_tasks(tasks: Iterable[asyncio.Future[Any]]) -> None:
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def write_out(self, name: str, message: IPBuilder) -> bool:
        if self._stop_requested.is_set():
            return False

        port = self.out_ports.get(name)
        if port is None:
            return False

        try:
            await port.write(value=message)
            return True
        except capnp.KjException as e:
            logger.error("%s RPC exception writing output port '%s': %s", self.name, name, getattr(e, "description", e))
            self.out_ports[name] = None
            return False

    async def write_array_out(
        self,
        name: str,
        strategy: ArrayOutStrategy | str,
        message: IPBuilder,
    ) -> bool:
        if self._stop_requested.is_set():
            return False

        ports = self.array_out_ports.get(name)
        if not ports:
            return False

        strategy = ArrayOutStrategy(strategy)
        if strategy == ArrayOutStrategy.BROADCAST:
            wrote_any = False
            for i, port in enumerate(ports):
                if port is not None:
                    wrote_any = await self._write_array_out_port(name, i, port, message) or wrote_any
            return wrote_any

        if strategy != ArrayOutStrategy.ROUND_ROBIN:
            raise ValueError(f"Unsupported array output strategy: {strategy}")

        start_index = self._array_out_next_indices.get(name, 0)
        for offset in range(len(ports)):
            port_index = (start_index + offset) % len(ports)
            port = ports[port_index]
            if port is None:
                continue

            self._array_out_next_indices[name] = (port_index + 1) % len(ports)
            if await self._write_array_out_port(name, port_index, port, message):
                return True

        return False

    async def _write_array_out_port(
        self,
        name: str,
        port_index: int,
        port: WriterClient,
        message: IPBuilder,
    ) -> bool:
        try:
            await port.write(value=message)
            return True
        except capnp.KjException as e:
            logger.error(
                "%s RPC exception writing array output port '%s[%s]': %s",
                self.name,
                name,
                port_index,
                getattr(e, "description", e),
            )
            self.array_out_ports[name][port_index] = None
            return False

    async def close_in_ports(self):
        for name, port in self.in_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.in_ports[name] = None
                    logger.info("closed in port '%s'", name)
                except (capnp.KjException, RuntimeError) as e:
                    logger.error("%s: Exception closing in port '%s': %s", os.path.basename(__file__), name, e)
        for name, ports in self.array_in_ports.items():
            for i, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[i] = None
                        logger.info("closed array in port '%s[%s]'", name, i)
                    except (capnp.KjException, RuntimeError) as e:
                        logger.error("Exception closing array in port '%s[%s]': %s", name, i, e)

    async def force_close_ports(self):
        await self.close_in_ports()
        await self.close_out_ports()

    async def close_out_ports(self):
        for name, port in self.out_ports.items():
            if port is not None:
                try:
                    await port.close()
                    self.out_ports[name] = None
                    logger.info("closed out port '%s'", name)
                except (capnp.KjException, RuntimeError) as e:
                    logger.error("%s: Exception closing out port '%s': %s", os.path.basename(__file__), name, e)
        for name, ports in self.array_out_ports.items():
            for i, port in enumerate(ports):
                if port is not None:
                    try:
                        await port.close()
                        ports[i] = None
                        logger.info("closed array out port '%s[%s]'", name, i)
                    except (capnp.KjException, RuntimeError) as e:
                        logger.error("Exception closing array out port '%s[%s]': %s", name, i, e)

    async def serve(
        self,
        writer_sr: str | None = None,
        serve_bootstrap: bool = False,
        host: str | None = None,
        port: int | None = None,
    ):
        if writer_sr and len(writer_sr) > 0 and (writer_cap := await self.con_man.try_connect(writer_sr)) is not None:
            writer = writer_cap.cast_as(fbp_capnp.Channel.Writer)
            await writer.write(value=self)
            logger.info("wrote process cap into %s", writer_sr)

        async def new_connection(stream: capnp.AsyncIoStream):
            await capnp.TwoPartyServer(stream, bootstrap=self if serve_bootstrap else None).on_disconnect()

        port = port or 0
        server = await capnp.AsyncIoStream.create_server(new_connection, host, port)
        try:
            async with server:
                if serve_bootstrap:
                    host = host or common.get_public_ip()
                    import socket

                    ipv4_sockets = list(filter(lambda s: s.family == socket.AddressFamily.AF_INET, server.sockets))
                    if len(ip4_socks := ipv4_sockets) > 0:
                        port = ip4_socks[0].getsockname()[1]
                    logger.info("Process(%s) SR: capnp://%s:%s", self.name, host, port)
                await server.serve_forever()
        finally:
            await self.force_close_ports()
            await self.transition_to_state("closed")


def start_local_process_component(
    path_to_executable,
    process_cap_writer_sr,
    name: str | None = None,
    log_level: str | None = None,
) -> sp.Popen[str]:
    pte_split = list(path_to_executable.split(" "))
    if len(pte_split) > 0 and (exe := pte_split[0]) and exe == "python":
        pte_split[0] = sys.executable
    proc = sp.Popen(
        pte_split
        + [process_cap_writer_sr]
        + ([f'--name="{name}"'] if name else [])
        + ([f"--log_level={log_level}"] if log_level else []),
        # stdout=sp.PIPE, stderr=sp.STDOUT,
        text=True,
    )
    return proc


def create_default_args_parser(
    component_description: str,
):
    parser = argparse.ArgumentParser(description=component_description)
    _ = parser.add_argument(
        "process_cap_writer_sr",
        type=str,
        nargs="?",
        help="SturdyRef to the Writer[fbp.capnp:Process]. Writes process capability on startup to writer.",
    )
    _ = parser.add_argument(
        "--output_json_default_config",
        "-o",
        action="store_true",
        help="Output JSON configuration file with default settings at commandline. To be used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--output_json_component_metadata",
        "-O",
        action="store_true",
        help="Output JSON component metadata at commandline. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "--write_json_default_config",
        "-w",
        type=str,
        help="Output JSON configuration file with default settings in the current directory. To used with IIP at 'conf' port.",
    )
    _ = parser.add_argument(
        "--write_json_component_metadata",
        "-W",
        type=str,
        help="Output JSON component metadata in the current directory. To be used for configuring component service.",
    )
    _ = parser.add_argument(
        "-b",
        "--serve_bootstrap",
        action="store_true",
        help="Serve process as the bootstrap object.>",
    )
    _ = parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to be used when serving the process.",
    )
    _ = parser.add_argument(
        "--name",
        "-n",
        type=str,
        help="Name of process to be started.",
    )
    add_log_level_argument(parser)
    return parser


def run_process_from_metadata_and_cmd_args(p: Process, component_meta):
    parser = create_default_args_parser(component_description=p.description)
    args = parser.parse_args()
    configure_logging(args.log_level)
    if component_meta:
        default_config = {
            k: v["value"] for k, v in component_meta.get("component", {}).get("defaultConfig", {}).items()
        }
    else:
        default_config = {}
    if args.name is not None:
        p.name = args.name
    if args.output_json_default_config:
        sys.stdout.write(json.dumps(default_config, indent=4) + "\n")
        exit(0)
    elif args.write_json_default_config:
        with open(args.write_json_default_config, "w") as _:
            json.dump(default_config, _, indent=4)
            exit(0)
    elif args.output_json_component_metadata:
        sys.stdout.write(json.dumps(component_meta, indent=4) + "\n")
        exit(0)
    elif args.write_json_component_metadata:
        with open(args.write_json_component_metadata, "w") as _:
            json.dump(component_meta, _, indent=4)
            exit(0)
    if args.process_cap_writer_sr:
        asyncio.run(
            capnp.run(
                p.serve(
                    writer_sr=args.process_cap_writer_sr,
                    serve_bootstrap=args.serve_bootstrap,
                    host=args.host,
                    port=args.port,
                ),
            ),
        )
    else:
        logger.error("A sturdy ref to a writer capability is necessary to start the process.")
