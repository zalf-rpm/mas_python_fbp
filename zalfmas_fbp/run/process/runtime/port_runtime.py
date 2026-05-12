from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast, override

import capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.fbp.fbp_capnp.types.results.tuples import (
    ConnectinportResultTuple,
    ConnectoutportResultTuple,
)
from zalfmas_common import common

from zalfmas_fbp.run.metadata import ComponentMetadata
from zalfmas_fbp.run.process.context import ProcessPortState
from zalfmas_fbp.run.process.types import ArrayReaderPorts, ArrayWriterPorts

from .output_runtime import OutputRuntime

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, WriterClient
    from mas.schema.persistence.persistence_capnp.types.readers import SturdyRefReader

    from zalfmas_fbp.run.metadata import ComponentPortMetadata

type ConnectedPort = ReaderClient | WriterClient
type StandardPortMap = dict[str, ReaderClient | None] | dict[str, WriterClient | None]
type ArrayPortMap = dict[str, ArrayReaderPorts] | dict[str, ArrayWriterPorts]

logger = logging.getLogger(__name__)


class PortDisconnect(fbp_capnp.Process.Disconnect.Server):
    def __init__(
        self,
        ports: StandardPortMap | ArrayPortMap,
        name: str,
        port: ConnectedPort | None,
        index: int | None = None,
    ):
        if index is None:
            self.standard_ports: StandardPortMap | None = cast("StandardPortMap", ports)
            self.array_ports: ArrayPortMap | None = None
        else:
            self.standard_ports = None
            self.array_ports = cast("ArrayPortMap", ports)
        self.name: str = name
        self.port: ConnectedPort | None = port
        self.index: int | None = index
        self.disconnected: bool = False

    @override
    async def disconnect(self, _context, **_kwargs) -> bool:
        if self.disconnected or self.port is None:
            return False

        if self._disconnect_port():
            await self._close_port()
            self.disconnected = True
            return True
        return False

    def _disconnect_port(self) -> bool:
        if self.index is None:
            return self._disconnect_standard_port()
        return self._disconnect_array_port()

    def _disconnect_standard_port(self) -> bool:
        if self.standard_ports is None or self.standard_ports.get(self.name) is not self.port:
            return False
        self.standard_ports[self.name] = None
        return True

    def _disconnect_array_port(self) -> bool:
        if self.array_ports is None or self.index is None:
            return False

        array_ports = self.array_ports.get(self.name)
        if array_ports is None:
            return False

        if self.index < len(array_ports) and array_ports[self.index] is self.port:
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
        try:
            await self.port.close()
        except (capnp.KjException, RuntimeError):
            logger.exception("Exception closing disconnected port '%s'", self.name)


class ProcessPortRuntime:
    def __init__(
        self,
        *,
        metadata: ComponentMetadata | None,
        ports: ProcessPortState,
        con_man: common.ConnectionManager,
        output_runtime: OutputRuntime,
    ) -> None:
        self._metadata: ComponentMetadata | None = metadata
        self._ports: ProcessPortState = ports
        self._con_man: common.ConnectionManager = con_man
        self._output_runtime: OutputRuntime = output_runtime

    @staticmethod
    def _port_message(name: str, port_info: ComponentPortMetadata | None, port_type: str) -> dict[str, str]:
        return {
            "name": name,
            "type": port_type,
            "contentType": port_info.contentType if port_info is not None else "Text",
        }

    def in_port_messages(self) -> list[dict[str, str]]:
        component_meta = self._metadata
        in_port_infos = {p.name: p for p in component_meta.inPorts} if component_meta is not None else {}
        ports = [self._port_message(k, in_port_infos.get(k), "standard") for k in self._ports.in_ports]
        ports.extend(self._port_message(k, in_port_infos.get(k), "array") for k in self._ports.array_in_ports)
        return ports

    async def connect_in_port(self, name: str, sturdy_ref: SturdyRefReader) -> ConnectinportResultTuple:
        reader = (
            reader_cap.cast_as(fbp_capnp.Channel.Reader)
            if (reader_cap := await self._con_man.try_connect(sturdy_ref)) is not None
            else None
        )
        ports = self._ports
        if name in ports.array_in_ports:
            index = len(ports.array_in_ports[name])
            ports.array_in_ports[name].append(reader)
            _ = ports.array_in_buffers.setdefault(name, {})
            return ConnectinportResultTuple(
                reader is not None,
                PortDisconnect(ports.array_in_ports, name, reader, index),
            )

        ports.in_ports[name] = reader
        return ConnectinportResultTuple(
            ports.in_ports[name] is not None,
            PortDisconnect(ports.in_ports, name, reader),
        )

    def out_port_messages(self) -> list[dict[str, str]]:
        component_meta = self._metadata
        out_port_infos = {p.name: p for p in component_meta.outPorts} if component_meta is not None else {}
        ports = [self._port_message(k, out_port_infos.get(k), "standard") for k in self._ports.out_ports]
        ports.extend(self._port_message(k, out_port_infos.get(k), "array") for k in self._ports.array_out_ports)
        return ports

    async def connect_out_port(self, name: str, sturdy_ref: SturdyRefReader) -> ConnectoutportResultTuple:
        writer = (
            writer_cap.cast_as(fbp_capnp.Channel.Writer)
            if (writer_cap := await self._con_man.try_connect(sturdy_ref)) is not None
            else None
        )
        ports = self._ports
        if name in ports.array_out_ports:
            index = len(ports.array_out_ports[name])
            ports.array_out_ports[name].append(writer)
            _ = ports.array_out_next_indices.setdefault(name, 0)
            _ = self._output_runtime.ensure_array_out_write_task_slots(name, ports.array_out_ports[name])
            return ConnectoutportResultTuple(
                writer is not None,
                PortDisconnect(ports.array_out_ports, name, writer, index),
            )

        ports.out_ports[name] = writer
        return ConnectoutportResultTuple(
            ports.out_ports[name] is not None,
            PortDisconnect(ports.out_ports, name, writer),
        )
