from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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


class ComponentMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: ComponentCategoryMetadata | None = None
    info: ComponentInfoMetadata
    type: Literal["process", "standard"]
    inPorts: list[ComponentPortMetadata] = Field(default_factory=list)
    outPorts: list[ComponentPortMetadata] = Field(default_factory=list)
    defaultConfig: dict[str, ComponentDefaultConfigEntry] = Field(default_factory=dict)

    def default_config_values(self) -> dict[str, Any]:
        return {key: entry.value for key, entry in self.defaultConfig.items()}
