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
"""Representation bridge: Cap'n Proto <-> Python <-> JSON (read side).

This is module S3 of ``base_components_plan.md``. It owns the knowledge of how to turn an
``AnyPointer`` into a plain Python value, so that components - and the selector language in
``selectors.py`` - never have to guess.

A note on why guessing is not allowed: Cap'n Proto validates a pointer's *kind* (text vs. struct)
but not *which* struct it holds. Once a struct has travelled through an ``AnyPointer`` field - which
is exactly what ``IP.content`` and ``IP.KV.value`` are - casting it to the wrong struct schema does
not fail, it silently reinterprets the bits::

    ip = fbp_capnp.IP.new_message(
        content=common_capnp.StructuredText.new_message(type="json", value='{"a":1}')
    )
    v = ip.as_reader().content.as_struct(common_capnp.Value)   # wrong schema
    v.which()   # -> 'f64'   (no exception)
    v.f64       # -> 5e-324

That is Cap'n Proto's forward-compatibility rule at work: reads past the end of the allocated data
section return defaults instead of erroring, so ``Value``'s union tag at bits [64, 80) reads 0
(the ``f64`` branch) and ``Value.f64`` at bits [0, 64) reads ``StructuredText.type``'s enumerant 1 as
a double bit pattern.

``as_text()`` *does* raise for a struct pointer, so it is a sound probe for "text or struct" - but
nothing distinguishes one struct from another. Every read here is therefore driven by an explicit
type - the attribute's ``valueType``, the IP's ``sysAttributes.contentType``, or a type supplied by
component config - and falls back to :data:`MISSING` rather than to a guess.
"""

from __future__ import annotations

import json
import logging
import tomllib
from typing import TYPE_CHECKING, Any, Final

import capnp
from mas.schema.common import common_capnp
from zalfmas_common import common

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)


class _Missing:
    """Sentinel for 'this could not be resolved', distinct from a resolved ``None``."""

    _instance: _Missing | None = None

    def __new__(cls) -> _Missing:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Final[_Missing] = _Missing()

VALUE_TYPE: Final[str] = "@0xe17592335373b246 = common/common.capnp:Value"
STRUCTURED_TEXT_TYPE: Final[str] = "@0xed6c098b67cad454 = common/common.capnp:StructuredText"

_SCALAR_FIELDS: Final[frozenset[str]] = frozenset(
    {"f64", "f32", "i64", "i32", "i16", "i8", "ui64", "ui32", "ui16", "ui8", "b", "t"},
)
_SCALAR_LIST_FIELDS: Final[frozenset[str]] = frozenset(
    {"lf64", "lf32", "li64", "li32", "li16", "li8", "lui64", "lui32", "lui16", "lui8", "lb", "lt"},
)

_schema_cache: dict[str, Any] = {}


def resolve_schema(content_type: str | None) -> Any | None:
    """Resolve a Cap'n Proto content-type string to a schema, or ``None`` if it cannot be parsed.

    Caches both hits and misses - components call this per IP.
    """
    if not content_type:
        return None
    if content_type in _schema_cache:
        return _schema_cache[content_type]

    try:
        schema = common.schema_from_content_type_string(content_type)
    except (AttributeError, RuntimeError, TypeError, ValueError, capnp.KjException):
        logger.debug("Could not resolve schema from content type %r.", content_type, exc_info=True)
        schema = None

    _schema_cache[content_type] = schema
    return schema


def _is_value_schema(schema: Any) -> bool:
    return getattr(schema, "node", None) is not None and schema.node.id == common_capnp.Value.schema.node.id


def _is_structured_text_schema(schema: Any) -> bool:
    node = getattr(schema, "node", None)
    return node is not None and node.id == common_capnp.StructuredText.schema.node.id


def python_from_value(reader: Any) -> Any:
    """Convert a ``common.capnp:Value`` reader into a plain Python value.

    ``lpair`` becomes a dict, ``lv`` a list. ``cap``/``lcap``/``p`` have no Python equivalent and
    are returned as the raw reader, since a component may still want to forward them.
    """
    which = reader.which()

    if which in _SCALAR_FIELDS:
        return getattr(reader, which)
    if which == "d":
        return bytes(reader.d)
    if which in _SCALAR_LIST_FIELDS:
        return list(getattr(reader, which))
    if which == "ld":
        return [bytes(item) for item in reader.ld]
    if which == "lv":
        return [python_from_value(item) for item in reader.lv]
    if which == "lpair":
        # Value.lpair is List(Pair) with unbound generic parameters, so fst/snd arrive as
        # AnyPointer and have to be read explicitly rather than by attribute access.
        result: dict[str, Any] = {}
        for pair in reader.lpair:
            try:
                key = pair.fst.as_text()
            except capnp.KjException:
                logger.debug("Skipping Value.lpair entry whose key is not Text.")
                continue
            try:
                result[key] = python_from_value(pair.snd.as_struct(common_capnp.Value))
            except capnp.KjException:
                result[key] = pair.snd
        return result

    # cap, lcap, p: no Python representation, hand back the reader unchanged
    return getattr(reader, which)


def python_from_structured_text(reader: Any) -> Any:
    """Parse a ``common.capnp:StructuredText`` into Python, honouring its ``type`` tag."""
    text = reader.value
    text_type = str(reader.type)
    try:
        if text_type == "json":
            return json.loads(text)
        if text_type == "toml":
            return tomllib.loads(text)
    except (json.JSONDecodeError, tomllib.TOMLDecodeError):
        logger.debug("StructuredText tagged %r did not parse; returning the raw text.", text_type)
    return text


def python_from_struct(reader: Any, schema: Any) -> Any:
    """Convert a struct reader of a known schema into plain Python."""
    if _is_value_schema(schema):
        return python_from_value(reader)
    if _is_structured_text_schema(schema):
        return python_from_structured_text(reader)
    try:
        return reader.to_dict()
    except (capnp.KjException, AttributeError, TypeError, ValueError):
        logger.debug("to_dict() failed for schema %r; returning the reader.", schema, exc_info=True)
        return reader


def python_from_any(pointer: Any, content_type: str | None = None) -> Any:
    """Convert an ``AnyPointer`` to Python using ``content_type``, or :data:`MISSING`.

    Without a usable ``content_type`` the only safe probe is ``as_text()`` - it genuinely raises for
    struct pointers, whereas a struct-to-struct cast would silently misread (see module docstring).
    JSON/TOML text is *not* parsed here; that is the caller's decision, since a component may want
    the text itself.
    """
    schema = resolve_schema(content_type)
    if schema is not None and getattr(schema, "node", None) is not None:
        node_type = schema.node.which()
        if node_type == "struct":
            try:
                return python_from_struct(pointer.as_struct(schema), schema)
            except capnp.KjException:
                logger.debug("Content did not read as %r.", content_type, exc_info=True)
                return MISSING
        if node_type == "enum":
            try:
                return str(pointer.as_enum(schema))
            except capnp.KjException:
                return MISSING
        if node_type == "interface":
            try:
                return pointer.as_interface(schema)
            except capnp.KjException:
                return MISSING

    try:
        return pointer.as_text()
    except capnp.KjException:
        return MISSING


def python_from_attr(kv: Any, type_hint: str | None = None) -> Any:
    r"""Read one ``IP.KV`` attribute as a plain Python value, or :data:`MISSING`.

    Implements decision D4 ("write ``common.Value``, read both"):

    1. ``valueType`` set  -> resolve that schema and read it. The reliable path, and the reason base
       components must always set ``valueType`` when writing.
    2. otherwise Text     -> a raw string attribute, as older components write them.
    3. otherwise          -> an untyped struct, which cannot be read without guessing, so
       :data:`MISSING`. Pass ``type_hint`` (from component config, as ``update_json.types`` does)
       to resolve these.
    """
    if kv._has("valueType") and kv.valueType:  # noqa: SLF001 - capnp readers expose _has only
        resolved = python_from_any(kv.value, kv.valueType)
        if resolved is not MISSING:
            return resolved

    try:
        return kv.value.as_text()
    except capnp.KjException:
        pass

    if type_hint:
        return python_from_any(kv.value, type_hint)
    return MISSING


def attr_reader(ip: IPReader, name: str) -> Any | None:
    """Return the ``IP.KV`` reader for ``name``, or ``None``."""
    if not ip.attributes:
        return None
    for kv in ip.attributes:
        if kv.key == name:
            return kv
    return None


def content_type_of(ip: IPReader) -> str | None:
    """The IP's declared content type, or ``None`` when unset."""
    content_type = ip.sysAttributes.contentType
    return content_type or None


def python_from_content(ip: IPReader, content_type: str | None = None) -> Any:
    """Read an IP's content as Python. The IP's own content type wins over ``content_type``.

    That precedence matches ``string/to_string``, where the configured type is a fallback for IPs
    that arrive untagged.
    """
    return python_from_any(ip.content, content_type_of(ip) or content_type)
