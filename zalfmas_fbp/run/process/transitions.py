from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, cast, override

import capnp
from mas.schema.fbp import fbp_capnp

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import ActivityInfoBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, WriterClient
    from mas.schema.fbp.fbp_capnp.types.enums import ProcessStateEnum
    from mas.schema.fbp.fbp_capnp.types.readers import ActivityInfoReader

type ConnectedPort = ReaderClient | WriterClient
type ArrayReaderPorts = list["ReaderClient | None"]
type ArrayWriterPorts = list["WriterClient | None"]
type StandardPortMap = dict[str, ReaderClient | None] | dict[str, WriterClient | None]
type ArrayPortMap = dict[str, ArrayReaderPorts] | dict[str, ArrayWriterPorts]

logger = logging.getLogger(__name__)


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback: Callable[[ProcessStateEnum, ProcessStateEnum], None],
    ):
        self.callback: Callable[[ProcessStateEnum, ProcessStateEnum], None] = callback

    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class ActivityTransition(fbp_capnp.Process.ActivityTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ],
    ):
        self.callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ] = callback

    @override
    async def activityChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


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
        except (capnp.KjException, RuntimeError) as e:
            logger.error("Exception closing disconnected port '%s': %s", self.name, e)
