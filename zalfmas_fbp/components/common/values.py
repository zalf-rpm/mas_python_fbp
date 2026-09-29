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

import base64
import json
import logging
import math
import tomllib
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

import capnp
from mas.schema.common import common_capnp
from zalfmas_common import common

if TYPE_CHECKING:
    from mas.schema.common.common_capnp.types.builders import ValueBuilder
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


# --------------------------------------------------------------------------------------------
# Write side: Python -> Cap'n Proto
# --------------------------------------------------------------------------------------------

_INT_RANGES: Final[dict[str, tuple[int, int]]] = {
    "i8": (-128, 127),
    "i16": (-32768, 32767),
    "i32": (-2147483648, 2147483647),
    "i64": (-9223372036854775808, 9223372036854775807),
    "ui8": (0, 255),
    "ui16": (0, 65535),
    "ui32": (0, 4294967295),
    "ui64": (0, 18446744073709551615),
}

_FLOAT_MAX: Final[dict[str, float]] = {"f32": 3.4028235e38, "f64": 1.7976931348623157e308}

_SMALLEST_SIGNED: Final[tuple[str, ...]] = ("i8", "i16", "i32", "i64")
_SMALLEST_UNSIGNED: Final[tuple[str, ...]] = ("ui8", "ui16", "ui32", "ui64", "i8", "i16", "i32", "i64")
_WIDEST_SIGNED: Final[tuple[str, ...]] = ("i64", "i32", "i16", "i8")
_WIDEST_UNSIGNED: Final[tuple[str, ...]] = ("ui64", "i64", "ui32", "i32", "ui16", "i16", "ui8", "i8")


def _value_message(field: str, payload: Any) -> ValueBuilder:
    """Build a one-field ``Value``.

    ``setattr`` rather than ``new_message(**{field: payload})``: the splat form makes a type checker
    try to match the payload against every union parameter, which buries real errors in noise.
    """
    message = common_capnp.Value.new_message()
    setattr(message, field, payload)
    return message


def value_fields() -> set[str]:
    """The union field names available on ``common.capnp:Value`` in the loaded schema."""
    return set(common_capnp.Value.schema.fieldnames)


def coerce_scalar_for_field(value: Any, field: str) -> Any:
    """Coerce a Python scalar into what ``Value.<field>`` accepts, or raise."""
    if field == "b":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false", "1", "0"):
            return value.strip().lower() in ("true", "1")
        msg = f"Cannot coerce {value!r} to bool"
        raise TypeError(msg)

    if field == "t":
        if isinstance(value, str):
            return value
        msg = f"Cannot coerce {value!r} to text"
        raise TypeError(msg)

    if field == "d":
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        msg = f"Cannot coerce {value!r} to data"
        raise TypeError(msg)

    if field in _INT_RANGES:
        if isinstance(value, bool):
            msg = "bool values are not accepted as integers"
            raise TypeError(msg)
        if isinstance(value, int):
            number = value
        elif isinstance(value, float) and value.is_integer():
            number = int(value)
        elif isinstance(value, str):
            number = _int_from_text(value, field)
        else:
            msg = f"Cannot coerce {value!r} to integer type {field}"
            raise TypeError(msg)
        low, high = _INT_RANGES[field]
        if low <= number <= high:
            return number
        msg = f"Integer {number} does not fit in {field}"
        raise ValueError(msg)

    if field in _FLOAT_MAX:
        if isinstance(value, bool):
            msg = "bool values are not accepted as float"
            raise TypeError(msg)
        if isinstance(value, (int, float)):
            number = float(value)
        elif isinstance(value, str):
            try:
                number = float(value.strip())
            except ValueError as exc:
                msg = f"Cannot coerce {value!r} to float type {field}"
                raise TypeError(msg) from exc
        else:
            msg = f"Cannot coerce {value!r} to float type {field}"
            raise TypeError(msg)
        if math.isnan(number) or math.isinf(number) or abs(number) <= _FLOAT_MAX[field]:
            return number
        msg = f"Float {number} does not fit in {field}"
        raise ValueError(msg)

    msg = f"Unsupported Value field: {field}"
    raise ValueError(msg)


def _int_from_text(text: str, field: str) -> int:
    stripped = text.strip()
    try:
        return int(stripped, 10)
    except ValueError:
        pass
    try:
        as_float = float(stripped)
    except ValueError as exc:
        msg = f"Cannot coerce {text!r} to integer type {field}"
        raise TypeError(msg) from exc
    if not as_float.is_integer():
        msg = f"Cannot coerce {text!r} to integer type {field}"
        raise TypeError(msg)
    return int(as_float)


def coerce_list_for_field(items: Sequence[Any], list_field: str) -> list[Any]:
    """Coerce a Python sequence into what ``Value.<list_field>`` accepts, or raise."""
    if not list_field.startswith("l"):
        msg = f"Field {list_field} is not a list field"
        raise ValueError(msg)
    return [coerce_scalar_for_field(item, list_field[1:]) for item in items]


def _numeric_field_for(kind: str, items: Sequence[Any], fields: set[str], smallest: bool) -> str:
    if kind == "float":
        candidates: tuple[str, ...] = ("f32", "f64") if smallest else ("f64", "f32")
    elif any(isinstance(item, int) and item < 0 for item in items):
        candidates = _SMALLEST_SIGNED if smallest else _WIDEST_SIGNED
    else:
        candidates = _SMALLEST_UNSIGNED if smallest else _WIDEST_UNSIGNED

    for candidate in candidates:
        if candidate not in fields:
            continue
        try:
            _ = [coerce_scalar_for_field(item, candidate) for item in items]
        except (TypeError, ValueError):
            continue
        return candidate
    return "f64" if kind == "float" else "i64"


def _kind_of(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, (bytes, bytearray)):
        return "bytes"
    msg = f"Unsupported scalar type: {type(value).__name__}"
    raise TypeError(msg)


def determine_scalar_field(value: Any, fields: set[str], smallest: bool = True) -> str:
    """Pick the ``Value`` union field that best fits a Python scalar."""
    kind = _kind_of(value)
    if kind == "bool":
        return "b"
    if kind == "str":
        return "t"
    if kind == "bytes":
        return "d"
    return _numeric_field_for(kind, [value], fields, smallest)


def determine_list_field(items: Sequence[Any], fields: set[str], smallest: bool = True) -> str:
    """Pick the ``Value`` list field that best fits a Python sequence, or raise for mixed types."""
    if not items:
        return "lf64"

    kinds = {_kind_of(item) for item in items}
    if kinds == {"bool"}:
        return "lb"
    if kinds == {"str"}:
        return "lt"
    if kinds == {"bytes"}:
        return "ld"
    if kinds - {"int", "float"}:
        msg = f"List contains mixed incompatible types: {sorted(kinds)}"
        raise TypeError(msg)

    kind = "float" if "float" in kinds else "int"
    return f"l{_numeric_field_for(kind, items, fields, smallest)}"


def value_from_python(
    obj: Any,
    requested_type: str | None = None,
    auto_select: bool = True,
    smallest: bool = True,
    allow_fallback: bool = True,
) -> ValueBuilder:
    """Build a ``common.capnp:Value`` from a plain Python value.

    Dicts become ``lpair``, lists a typed list field (or ``lv`` for mixed content), scalars the
    smallest fitting field unless ``smallest`` is false. ``requested_type`` forces a union field and,
    when it does not fit, either falls back to a fitting one or raises depending on ``allow_fallback``.

    ``None`` has no ``Value`` representation and raises - a caller that needs one (as
    ``json/json_to_common_value`` does) must substitute a sentinel first.
    """
    fields = value_fields()
    return _value_from_python(obj, fields, requested_type, auto_select, smallest, allow_fallback)


def _value_from_python(
    obj: Any,
    fields: set[str],
    requested_type: str | None,
    auto_select: bool,
    smallest: bool,
    allow_fallback: bool,
) -> ValueBuilder:
    if obj is None:
        msg = "common.capnp:Value cannot represent None; substitute a sentinel first"
        raise ValueError(msg)

    requested = requested_type if requested_type not in (None, "auto") else None

    if isinstance(obj, Mapping):
        if requested is not None and requested != "lpair" and not allow_fallback:
            msg = f"Requested type {requested!r} cannot hold object input (requires lpair)."
            raise ValueError(msg)
        pairs = [
            common_capnp.Pair.new_message(
                fst=str(key),
                snd=_value_from_python(item, fields, None, auto_select, smallest, allow_fallback),
            )
            for key, item in obj.items()
        ]
        return _value_message("lpair", pairs)

    is_list = isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray))

    if requested is not None:
        try:
            return _value_for_requested_type(obj, fields, requested, is_list, smallest, allow_fallback)
        except (TypeError, ValueError):
            if not allow_fallback:
                raise
            logger.warning("Requested type %r cannot represent %r; falling back to a fitting type.", requested, obj)

    if not auto_select:
        msg = "No usable requested_type and auto_select is disabled."
        raise ValueError(msg)

    if is_list:
        try:
            field = determine_list_field(list(obj), fields, smallest)
            return _value_message(field, coerce_list_for_field(list(obj), field))
        except TypeError:
            if "lv" not in fields:
                raise
            items = [_value_from_python(item, fields, None, auto_select, smallest, allow_fallback) for item in obj]
            return _value_message("lv", items)

    field = determine_scalar_field(obj, fields, smallest)
    return _value_message(field, coerce_scalar_for_field(obj, field))


def _value_for_requested_type(
    obj: Any,
    fields: set[str],
    requested: str,
    is_list: bool,
    smallest: bool,
    allow_fallback: bool,
) -> ValueBuilder:
    if requested not in fields:
        msg = f"Requested type {requested!r} is not available in the common.capnp:Value schema."
        raise ValueError(msg)

    if is_list:
        if requested == "lv":
            items = [_value_from_python(item, fields, None, True, smallest, allow_fallback) for item in obj]
            return _value_message("lv", items)
        if not requested.startswith("l"):
            msg = f"Requested scalar type {requested!r} cannot hold list input."
            raise ValueError(msg)
        return _value_message(requested, coerce_list_for_field(list(obj), requested))

    if requested.startswith("l"):
        msg = f"Requested list type {requested!r} cannot hold scalar input."
        raise ValueError(msg)
    return _value_message(requested, coerce_scalar_for_field(obj, requested))


# --------------------------------------------------------------------------------------------
# Struct <-> JSON-compatible Python
# --------------------------------------------------------------------------------------------

_INT_TYPE_TO_FIELD: Final[dict[str, str]] = {
    "int8": "i8",
    "int16": "i16",
    "int32": "i32",
    "int64": "i64",
    "uint8": "ui8",
    "uint16": "ui16",
    "uint32": "ui32",
    "uint64": "ui64",
    "float32": "f32",
    "float64": "f64",
}


def _is_capnp_object(value: Any) -> bool:
    return type(value).__module__.startswith("capnp")


def _encode_bytes(raw: bytes, data_as: str) -> Any:
    if data_as == "hex":
        return raw.hex()
    if data_as == "list":
        return list(raw)
    return base64.b64encode(raw).decode("ascii")


def _normalise_for_json(value: Any, data_as: str, unresolved: str) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return _encode_bytes(bytes(value), data_as)
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            normalised = _normalise_for_json(item, data_as, unresolved)
            if normalised is not MISSING:
                result[str(key)] = normalised
        return result
    if isinstance(value, (list, tuple)):
        items = [_normalise_for_json(item, data_as, unresolved) for item in value]
        return [item for item in items if item is not MISSING]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    if _is_capnp_object(value):
        # An AnyPointer or capability that to_dict() could not flatten. Text is recoverable;
        # anything else needs a type we do not have here (see D14).
        try:
            return value.as_text()
        except (capnp.KjException, AttributeError):
            pass
        if unresolved == "null":
            return None
        if unresolved == "repr":
            return repr(value)
        return MISSING

    return str(value)


def json_from_capnp(
    reader: Any,
    schema: Any = None,
    data_as: str = "base64",
    unresolved: str = "drop",
) -> Any:
    """Convert a Cap'n Proto struct reader into JSON-compatible Python.

    ``data_as`` encodes ``Data`` fields (``base64`` | ``hex`` | ``list``); ``unresolved`` decides what
    happens to pointers that carry no recoverable type (``drop`` | ``null`` | ``repr``).

    ``common.capnp:Value`` is special-cased because ``to_dict()`` leaves ``lpair``'s generic
    ``fst``/``snd`` as opaque pointers.
    """
    if schema is not None and _is_value_schema(schema):
        return _normalise_for_json(python_from_value(reader), data_as, unresolved)
    try:
        as_dict = reader.to_dict()
    except (capnp.KjException, AttributeError, TypeError, ValueError):
        logger.debug("to_dict() failed; falling back to the raw reader.", exc_info=True)
        return MISSING
    return _normalise_for_json(as_dict, data_as, unresolved)


def _field_type_name(schema: Any, name: str) -> str | None:
    try:
        return schema.fields[name].proto.slot.type.which()
    except (KeyError, AttributeError, capnp.KjException):
        return None


def _field_schema(schema: Any, name: str) -> Any | None:
    try:
        return schema.fields[name].schema
    except (KeyError, AttributeError, capnp.KjException):
        return None


def _coerce_for_field_type(value: Any, type_name: str, coerce_numbers: bool) -> Any:
    if not coerce_numbers:
        return value
    if type_name in _INT_TYPE_TO_FIELD and isinstance(value, (str, int, float, bool)):
        try:
            return coerce_scalar_for_field(value, _INT_TYPE_TO_FIELD[type_name])
        except (TypeError, ValueError):
            return value
    if type_name == "text" and isinstance(value, (int, float, bool)):
        return str(value)
    if type_name == "data" and isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except (ValueError, TypeError):
            return value
    return value


def _prepare_for_schema(value: Any, schema: Any, unknown_fields: str, coerce_numbers: bool) -> Any:
    if not isinstance(value, Mapping) or schema is None:
        return value

    known = set(getattr(schema, "fieldnames", ()) or ())
    prepared: dict[str, Any] = {}
    for key, item in value.items():
        name = str(key)
        if known and name not in known:
            if unknown_fields == "error":
                msg = f"{schema.node.displayName} has no field {name!r}"
                raise ValueError(msg)
            logger.debug("Ignoring unknown field %r for %s.", name, schema.node.displayName)
            continue

        type_name = _field_type_name(schema, name)
        if type_name == "struct":
            prepared[name] = _prepare_for_schema(item, _field_schema(schema, name), unknown_fields, coerce_numbers)
        elif type_name == "list" and isinstance(item, (list, tuple)):
            element_schema = getattr(_field_schema(schema, name), "elementType", None)
            prepared[name] = [
                _prepare_for_schema(element, element_schema, unknown_fields, coerce_numbers)
                if isinstance(element, Mapping)
                else element
                for element in item
            ]
        elif type_name == "anyPointer":
            # JSON carries no type for an AnyPointer, so only text can be stored faithfully.
            if item is None:
                continue
            if isinstance(item, str):
                prepared[name] = item
                continue
            msg = (
                f"Field {name!r} of {schema.node.displayName} is an AnyPointer, whose type JSON "
                f"cannot describe. Build it separately for its own type, or omit the field."
            )
            raise ValueError(msg)
        elif type_name is not None:
            prepared[name] = _coerce_for_field_type(item, type_name, coerce_numbers)
        else:
            prepared[name] = item

    return prepared


def capnp_from_json(
    obj: Any,
    schema: Any,
    unknown_fields: str = "error",
    coerce_numbers: bool = True,
) -> Any:
    """Build a Cap'n Proto struct builder of ``schema`` from JSON-compatible Python.

    ``unknown_fields`` is ``error`` or ``ignore``; ``coerce_numbers`` accepts ``"5"`` for an integer
    field, an int for a float field, and base64 text for a ``Data`` field. ``common.capnp:Value``
    targets are delegated to :func:`value_from_python`, which picks a fitting union field.
    """
    if _is_value_schema(schema):
        return value_from_python(obj)

    prepared = _prepare_for_schema(obj, schema, unknown_fields, coerce_numbers)
    if not isinstance(prepared, Mapping):
        msg = f"Cannot build {schema.node.displayName} from {type(obj).__name__}; expected an object."
        raise TypeError(msg)

    # A _StructSchema (what resolve_schema returns) has no new_message; that lives on the module
    # level type. Building the root off a message builder works from the schema alone.
    builder = capnp._MallocMessageBuilder().init_root(schema)
    try:
        builder.from_dict(prepared)
    except capnp.KjException as exc:
        msg = f"Could not build {schema.node.displayName} from the given object: {exc}"
        raise ValueError(msg) from exc
    return builder
