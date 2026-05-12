from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.clients import ReaderClient, WriterClient

type ArrayReaderPorts = list["ReaderClient | None"]
type ArrayWriterPorts = list["WriterClient | None"]
type ArrayOutWriteTasks = list["asyncio.Task[bool] | None"]
type ConfigScalar = str | int | float | bool
type ConfigValue = ConfigScalar | list[ConfigValue] | dict[str, ConfigValue]
type RawConfig = dict[str, ConfigValue]


class ArrayInStrategy(StrEnum):
    ZIP = "zip"
    NEXT_AVAILABLE = "next_available"


class ArrayOutStrategy(StrEnum):
    BROADCAST = "broadcast"
    ROUND_ROBIN = "round_robin"
    NEXT_AVAILABLE = "next_available"


class ProcessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
