from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

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
