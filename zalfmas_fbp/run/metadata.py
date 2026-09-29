from __future__ import annotations

from enum import Enum
from types import UnionType
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined


class ComponentCategoryMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str | None = None


class ComponentInfoMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    description: str | None = None


type PortRole = Literal["data", "config", "log", "error", "reject", "control"]

#: Port names reserved for a fixed role, so the convention holds even without tooling.
RESERVED_PORT_ROLES: dict[str, PortRole] = {
    "conf": "config",
    "log": "log",
    "err": "error",
    "rej": "reject",
}

#: Ports the runtime owns and injects itself; components must not declare them.
CONFIG_PORT_NAME = "conf"
LOG_PORT_NAME = "log"


class ComponentPortMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["array"] | None = None
    contentType: str = "Text"
    desc: str | None = None
    role: PortRole = "data"
    """What kind of port this is, so tooling can treat whole classes of them alike."""
    required: bool = False
    """Whether the component needs this port connected to work."""

    @model_validator(mode="after")
    def apply_reserved_role(self) -> ComponentPortMetadata:
        reserved = RESERVED_PORT_ROLES.get(self.name)
        if reserved is not None:
            if self.role == "data":
                self.role = reserved
            elif self.role != reserved:
                msg = f"Port {self.name!r} is reserved for role {reserved!r}, but declares {self.role!r}."
                raise ValueError(msg)
        elif self.role in ("config", "log"):
            names = [name for name, role in RESERVED_PORT_ROLES.items() if role == self.role]
            msg = f"Role {self.role!r} is reserved for a port named {names[0]!r}, not {self.name!r}."
            raise ValueError(msg)
        return self


class ComponentDefaultConfigEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: Any = None
    type: str | list[str] | None = None
    desc: str | None = None


def _strip_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin not in (Union, UnionType):
        return annotation

    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return args[0]
    return annotation


def _config_type_from_annotation(annotation: Any) -> str | list[str] | None:
    annotation = _strip_optional(annotation)
    origin = get_origin(annotation)
    if origin is Literal:
        values: list[str] = []
        for value in get_args(annotation):
            if isinstance(value, Enum):
                value = value.value
            values.append(str(value))
        return values

    if origin is list:
        args = get_args(annotation)
        if not args:
            return "list"
        inner = _config_type_from_annotation(args[0])
        if isinstance(inner, str):
            return f"list[{inner}]"
        return "list"

    if origin is dict:
        return "object"

    if annotation is str:
        return "string"
    if annotation is bool:
        return "bool"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return [str(member.value) for member in annotation]

    return None


def _config_default_value(field: FieldInfo) -> Any:
    if field.default is not PydanticUndefined:
        return field.default
    if field.default_factory is not None:
        return field.default_factory()
    return PydanticUndefined


def default_config_from_model(config_model: type[BaseModel]) -> dict[str, ComponentDefaultConfigEntry]:
    default_config: dict[str, ComponentDefaultConfigEntry] = {}
    for name, field in config_model.model_fields.items():
        default_value = _config_default_value(field)
        if default_value is PydanticUndefined:
            continue
        default_config[name] = ComponentDefaultConfigEntry(
            value=default_value,
            type=_config_type_from_annotation(field.annotation),
            desc=field.description,
        )
    return default_config


class ComponentMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    category: ComponentCategoryMetadata | None = None
    info: ComponentInfoMetadata
    type: Literal["process", "standard"]
    inPorts: list[ComponentPortMetadata] = Field(default_factory=list)
    outPorts: list[ComponentPortMetadata] = Field(default_factory=list)
    defaultConfig: dict[str, ComponentDefaultConfigEntry] = Field(default_factory=dict)
    config: type[BaseModel] | None = Field(default=None, exclude=True, repr=False)

    @model_validator(mode="after")
    def add_runtime_owned_ports(self) -> ComponentMetadata:
        """Give every Process component the runtime-owned ``conf`` and ``log`` ports.

        Components do not declare these - they belong to the runtime's contract, not the
        component's - but they have to appear in ``inPorts``/``outPorts`` so the runtime can
        connect them and the flow editor can show them. A component that still declares ``conf``
        keeps its own entry; only the role is enforced.
        """
        if self.type != "process":
            return self

        if not any(port.name == CONFIG_PORT_NAME for port in self.inPorts):
            self.inPorts = [
                *self.inPorts,
                ComponentPortMetadata(
                    name=CONFIG_PORT_NAME,
                    contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
                    desc="Runtime-owned. Configuration updates, applied between IPs.",
                    role="config",
                ),
            ]

        if not any(port.name == LOG_PORT_NAME for port in self.outPorts):
            self.outPorts = [
                *self.outPorts,
                ComponentPortMetadata(
                    name=LOG_PORT_NAME,
                    contentType="@0xdf6f09e80adf0ac2 = fbp/fbp.capnp:LogMessage",
                    desc="Runtime-owned. Log records, if connected; never blocks the component.",
                    role="log",
                ),
            ]
        return self

    @model_validator(mode="after")
    def derive_default_config(self) -> ComponentMetadata:
        if self.type != "process" or self.config is None:
            return self

        derived_default_config = default_config_from_model(self.config)
        if self.defaultConfig:
            derived_default_config.update(self.defaultConfig)
        self.defaultConfig = derived_default_config
        return self

    def default_config_values(self) -> dict[str, Any]:
        return {key: entry.value for key, entry in self.defaultConfig.items()}


Category = ComponentCategoryMetadata
Info = ComponentInfoMetadata
Port = ComponentPortMetadata
ConfigEntry = ComponentDefaultConfigEntry
Component = ComponentMetadata
