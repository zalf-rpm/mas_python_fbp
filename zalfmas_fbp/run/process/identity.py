from __future__ import annotations

from typing import Protocol, override


class ProcessIdentityContext(Protocol):
    @property
    def name(self) -> str | None: ...

    @property
    def id(self) -> str | None: ...


class WritableProcessIdentityContext(ProcessIdentityContext, Protocol):
    @property
    @override
    def name(self) -> str | None: ...

    @name.setter
    def name(self, name: str) -> None: ...

    @property
    def description(self) -> str | None: ...

    @description.setter
    def description(self, description: str) -> None: ...
