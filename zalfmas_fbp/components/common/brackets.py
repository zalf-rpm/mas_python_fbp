# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)
"""Substream (bracket) handling and full-fidelity attribute copying.

This is module S2 of ``base_components_plan.md``. It gives a name and one implementation to the
three things every substream-sensitive component currently re-implements:

- **Bracket transparency (D3).** A component that does not reason about substreams forwards bracket
  IPs unchanged. :class:`BracketPolicy` states that intent and :func:`handle_bracket` performs it.
- **Nesting-aware collection.** ``json/concat_json_substream``, ``ip/wrap_into_substream`` and
  ``ip/sort_ips`` each count nesting levels by hand; :func:`collect_substream` does it once and
  returns a :class:`Substream` tree.
- **Attribute copying.** ``IP.KV`` has optional ``desc`` and ``valueType`` Text fields, and reading
  an unset Text field yields ``""`` - writing that back turns "unset" into "explicitly empty", which
  downstream type resolution (D4/D14) reads as present. :func:`copy_attrs` and :func:`set_attrs`
  apply the ``_has()`` guard that ``ip/split_bracketed_stream`` documents by hand.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Literal

from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.components.common import selectors
from zalfmas_fbp.components.common.values import (
    VALUE_TYPE,
    python_from_attr,
    value_from_python,
)

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

OPEN_BRACKET: Final[str] = "openBracket"
CLOSE_BRACKET: Final[str] = "closeBracket"
STANDARD: Final[str] = "standard"
BRACKET_TYPES: Final[frozenset[str]] = frozenset({OPEN_BRACKET, CLOSE_BRACKET})

#: Attribute carrying a substream's IP count, as written by ``ip/split_bracketed_stream``.
SUBSTREAM_LENGTH_ATTR: Final[str] = "substream_length"

type ReadIP = Callable[[], Awaitable[IPReader | None]]
type WriteIP = Callable[[Any], Awaitable[bool]]


def is_bracket(ip: IPReader) -> bool:
    return str(ip.type) in BRACKET_TYPES


def is_open_bracket(ip: IPReader) -> bool:
    return str(ip.type) == OPEN_BRACKET


def is_close_bracket(ip: IPReader) -> bool:
    return str(ip.type) == CLOSE_BRACKET


# --------------------------------------------------------------------------------------------
# Attributes
# --------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Attr:
    """An attribute to write, with its Cap'n Proto type and optional description."""

    value: Any
    value_type: str | None = None
    desc: str | None = None


def _is_kv_reader(value: Any) -> bool:
    return hasattr(value, "key") and hasattr(value, "value") and hasattr(value, "_has")


def attr_readers(ip: IPReader) -> dict[str, Any]:
    """The IP's attributes as raw ``IP.KV`` readers, keyed by name."""
    if not ip.attributes:
        return {}
    return {kv.key: kv for kv in ip.attributes}


def attrs_as_dict(ip: IPReader, attr_types: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The IP's attributes as plain Python values (D4), keyed by name.

    Attributes that cannot be read without a type resolve to ``MISSING`` rather than a guess; pass
    ``attr_types`` to resolve those.
    """
    return {kv.key: python_from_attr(kv, selectors.attr_type_for(kv.key, attr_types)) for kv in (ip.attributes or [])}


def _write_attr(target: Any, name: str, value: Any) -> None:
    target.key = name

    if _is_kv_reader(value):
        # Only copy optional Text fields that are actually set: reading an unset Text field yields
        # "", and writing that back turns "unset" into "explicitly empty", which D4's type
        # resolution would then treat as present.
        if value._has("desc"):  # noqa: SLF001 - capnp readers expose presence only via _has
            target.desc = value.desc
        target.value = value.value
        if value._has("valueType"):  # noqa: SLF001
            target.valueType = value.valueType
        return

    if isinstance(value, Attr):
        target.value = value.value
        if value.value_type:
            target.valueType = value.value_type
        if value.desc:
            target.desc = value.desc
        return

    # D4: base components always write common.Value, with valueType set.
    target.value = value_from_python(value)
    target.valueType = VALUE_TYPE


def set_attrs(ip: IPBuilder, attrs: Mapping[str, Any]) -> None:
    """Write ``attrs`` onto ``ip``, replacing any attributes already there.

    Values may be ``IP.KV`` readers (copied with full fidelity), :class:`Attr` instances (written as
    given), or plain Python values (wrapped into a ``common.Value`` per D4).
    """
    entries = list(attrs.items())
    if not entries:
        return
    builders = ip.init("attributes", len(entries))
    for i, (name, value) in enumerate(entries):
        _write_attr(builders[i], name, value)


def copy_attrs(
    source: IPReader,
    target: IPBuilder,
    extra: Mapping[str, Any] | None = None,
    remove: Collection[str] = (),
) -> None:
    """Copy ``source``'s attributes onto ``target``, minus ``remove``, plus ``extra``.

    Unlike ``zalfmas_common.common.copy_and_set_fbp_attrs`` this preserves ``desc`` as well as
    ``valueType``, and applies any number of overrides rather than only the first match.
    """
    merged: dict[str, Any] = {name: kv for name, kv in attr_readers(source).items() if name not in remove}
    for name, value in (extra or {}).items():
        if name in remove:
            continue
        merged[name] = value
    set_attrs(target, merged)


def merge_attrs(ips: Sequence[IPReader], remove: Collection[str] = ()) -> dict[str, Any]:
    """Merge attributes across several IPs, later IPs winning. Returns ``IP.KV`` readers."""
    merged: dict[str, Any] = {}
    for ip in ips:
        for name, kv in attr_readers(ip).items():
            if name not in remove:
                merged[name] = kv
    return merged


def make_bracket(
    bracket_type: Literal["openBracket", "closeBracket"],
    attrs: Mapping[str, Any] | None = None,
) -> IPBuilder:
    """Build an open- or close-bracket IP, optionally carrying attributes."""
    if bracket_type not in BRACKET_TYPES:
        msg = f"{bracket_type!r} is not a bracket type; expected one of {sorted(BRACKET_TYPES)}"
        raise ValueError(msg)
    ip = fbp_capnp.IP.new_message(type=bracket_type)
    if attrs:
        set_attrs(ip, attrs)
    return ip


# --------------------------------------------------------------------------------------------
# Policy
# --------------------------------------------------------------------------------------------


class BracketPolicy(StrEnum):
    """How a component treats bracket IPs. TRANSPARENT is the library default (D3)."""

    TRANSPARENT = "transparent"
    """Forward bracket IPs unchanged; apply component logic to standard IPs only."""

    AWARE = "aware"
    """The component reasons about substreams itself (see :func:`collect_substream`)."""

    IGNORE = "ignore"
    """Drop bracket IPs, flattening the stream."""


class BracketOutcome(StrEnum):
    NOT_A_BRACKET = "not_a_bracket"
    HANDLED = "handled"
    WRITE_FAILED = "write_failed"


async def handle_bracket(ip: IPReader, policy: BracketPolicy, write: WriteIP) -> BracketOutcome:
    """Apply ``policy`` to ``ip`` if it is a bracket.

    Returns :attr:`BracketOutcome.NOT_A_BRACKET` when the caller should process the IP normally,
    :attr:`~BracketOutcome.HANDLED` when it should move on to the next IP, and
    :attr:`~BracketOutcome.WRITE_FAILED` when the output port is gone::

        match await handle_bracket(in_ip, BracketPolicy.TRANSPARENT, writer):
            case BracketOutcome.WRITE_FAILED:
                return
            case BracketOutcome.HANDLED:
                continue
            case BracketOutcome.NOT_A_BRACKET:
                ...
    """
    if not is_bracket(ip):
        return BracketOutcome.NOT_A_BRACKET
    if policy is BracketPolicy.IGNORE:
        return BracketOutcome.HANDLED
    if policy is BracketPolicy.AWARE:
        return BracketOutcome.NOT_A_BRACKET
    return BracketOutcome.HANDLED if await write(ip) else BracketOutcome.WRITE_FAILED


class BracketTracker:
    """Tracks substream nesting depth as IPs stream past."""

    def __init__(self, name: str = "") -> None:
        self._name: str = name
        self.depth: int = 0
        self.unbalanced_closes: int = 0

    def observe(self, ip: IPReader) -> int:
        """Account for one IP and return the nesting depth after it."""
        if is_open_bracket(ip):
            self.depth += 1
        elif is_close_bracket(ip):
            if self.depth == 0:
                self.unbalanced_closes += 1
                logger.warning("%s: close-bracket without a matching open-bracket; ignoring.", self._name or "stream")
            else:
                self.depth -= 1
        return self.depth

    @property
    def inside_substream(self) -> bool:
        return self.depth > 0


# --------------------------------------------------------------------------------------------
# Collection
# --------------------------------------------------------------------------------------------


@dataclass
class Substream:
    """One substream: its brackets, and its items in order (nested substreams included)."""

    open_ip: IPReader | None = None
    items: list[IPReader | Substream] = field(default_factory=list)
    close_ip: IPReader | None = None
    truncated: bool = False
    """True when the input closed before the matching close-bracket arrived."""

    @property
    def is_leaf(self) -> bool:
        """True when this substream contains no nested substreams."""
        return not any(isinstance(item, Substream) for item in self.items)

    @property
    def ips(self) -> list[IPReader]:
        """The directly contained standard IPs, excluding nested substreams."""
        return [item for item in self.items if not isinstance(item, Substream)]

    def all_ips(self) -> Iterator[IPReader]:
        """Every standard IP in this substream, depth first."""
        for item in self.items:
            if isinstance(item, Substream):
                yield from item.all_ips()
            else:
                yield item

    def leaves(self) -> Iterator[Substream]:
        """Every leaf substream at or below this one."""
        if self.is_leaf:
            yield self
            return
        for item in self.items:
            if isinstance(item, Substream):
                yield from item.leaves()

    def __len__(self) -> int:
        return len(self.items)


async def collect_substream(read: ReadIP, open_ip: IPReader | None = None) -> Substream:
    """Read one complete substream, nested substreams included.

    ``open_ip`` is the already-consumed open-bracket; pass ``None`` to have it read here (anything
    arriving before the open-bracket is then discarded with a warning). If the input closes first,
    the partial substream is returned with ``truncated`` set rather than raising - losing IPs that
    were already read is worse than an incomplete group.
    """
    if open_ip is None:
        while True:
            ip = await read()
            if ip is None:
                return Substream(truncated=True)
            if is_open_bracket(ip):
                open_ip = ip
                break
            logger.warning("Discarding IP of type %r received while waiting for an open-bracket.", str(ip.type))

    substream = Substream(open_ip=open_ip)
    while True:
        ip = await read()
        if ip is None:
            substream.truncated = True
            return substream
        if is_close_bracket(ip):
            substream.close_ip = ip
            return substream
        if is_open_bracket(ip):
            nested = await collect_substream(read, ip)
            substream.items.append(nested)
            if nested.truncated:
                substream.truncated = True
                return substream
            continue
        substream.items.append(ip)
