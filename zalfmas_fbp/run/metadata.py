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


class ComponentPortMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Literal["array"] | None = None
    contentType: str = "Text"
    desc: str | None = None


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
