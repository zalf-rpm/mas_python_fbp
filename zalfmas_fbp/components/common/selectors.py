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
r"""Selector and predicate mini-language shared by the base components.

This is module S1 of ``base_components_plan.md``. One way to say "this piece of this IP", used by
filter, route, group-by, dedupe, sort, join, reduce and format-string.

Decision D1 - a sigil marks a selector, everything else is a literal:

==========================  ====================================================================
``@attr``, ``@attr/sub/0``  an IP attribute, optionally with a path into its value
``.``, ``./a/b/0``          the IP content, optionally with a path into it
``#type``, ``#contentType`` IP metadata
``gjson:<query>``           an opt-in GJSON query against JSON content
anything else               a literal value
==========================  ====================================================================

A literal that really does start with a sigil is escaped with a leading backslash, so ``"\@zalf.de"``
is the literal ``@zalf.de``. Non-string config values (numbers, booleans, ``None``, lists) are
always literals.

Path segments that look like integers are list indices (D11), so ``./items/0`` indexes rather than
looking up the key ``"0"``. Comparisons coerce permissively (D8) unless ``strict_types`` is set,
because a TOML config cannot express Cap'n Proto's numeric types.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Self

import gjson
from pydantic import BaseModel, ConfigDict, Field, model_validator

from zalfmas_fbp.components.common.values import (
    MISSING,
    attr_reader,
    content_type_of,
    python_from_attr,
    python_from_content,
)

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

SIGIL_ATTRIBUTE: Final[str] = "@"
SIGIL_CONTENT: Final[str] = "."
SIGIL_META: Final[str] = "#"
PREFIX_GJSON: Final[str] = "gjson:"
ESCAPE: Final[str] = "\\"

DEFAULT_PATH_SEPARATOR: Final[str] = "/"

META_TYPE: Final[str] = "type"
META_CONTENT_TYPE: Final[str] = "contentType"
_META_NAMES: Final[frozenset[str]] = frozenset({META_TYPE, META_CONTENT_TYPE})

_INT_SEGMENT = re.compile(r"^-?\d+$")


class SelectorKind(StrEnum):
    ATTRIBUTE = "attribute"
    CONTENT = "content"
    META = "meta"
    GJSON = "gjson"
    LITERAL = "literal"


@dataclass(frozen=True)
class Selector:
    kind: SelectorKind
    raw: Any = None
    name: str = ""
    path: tuple[str | int, ...] = ()
    literal: Any = None
    query: str = ""

    @property
    def is_literal(self) -> bool:
        return self.kind is SelectorKind.LITERAL


def split_path(path: str, separator: str = DEFAULT_PATH_SEPARATOR) -> tuple[str | int, ...]:
    """Split a path into segments, turning integer-looking ones into list indices (D11)."""
    if not path:
        return ()
    if not separator:
        return (path,)

    segments: list[str | int] = []
    for part in path.split(separator):
        if part == "":
            continue
        segments.append(int(part) if _INT_SEGMENT.match(part) else part)
    return tuple(segments)


def parse_selector(spec: Any, separator: str = DEFAULT_PATH_SEPARATOR) -> Selector:
    r"""Parse a config value into a :class:`Selector`.

    Non-strings are literals. Strings are literals unless they start with a sigil; a leading
    backslash escapes a sigil that was meant literally (``"\@x"`` -> the literal ``"@x"``).
    """
    if not isinstance(spec, str):
        return Selector(kind=SelectorKind.LITERAL, raw=spec, literal=spec)

    if spec.startswith(ESCAPE):
        return Selector(kind=SelectorKind.LITERAL, raw=spec, literal=spec[1:])

    if spec.startswith(PREFIX_GJSON):
        return Selector(kind=SelectorKind.GJSON, raw=spec, query=spec[len(PREFIX_GJSON) :])

    if spec.startswith(SIGIL_ATTRIBUTE):
        name, _, rest = spec[1:].partition(separator) if separator else (spec[1:], "", "")
        return Selector(
            kind=SelectorKind.ATTRIBUTE,
            raw=spec,
            name=name,
            path=split_path(rest, separator),
        )

    if spec.startswith(SIGIL_CONTENT):
        return Selector(kind=SelectorKind.CONTENT, raw=spec, path=split_path(spec[1:], separator))

    if spec.startswith(SIGIL_META):
        return Selector(kind=SelectorKind.META, raw=spec, name=spec[1:])

    return Selector(kind=SelectorKind.LITERAL, raw=spec, literal=spec)


def _maybe_parse_json(value: Any) -> Any:
    """Parse a string as JSON so a path can be applied to it, else return it unchanged."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return value


def apply_path(value: Any, path: Sequence[str | int]) -> Any:
    """Walk ``path`` into ``value``, returning ``MISSING`` if any step does not exist.

    A string encountered while there is still path left is parsed as JSON first - that is what makes
    ``./a/b`` work on an IP whose content is a JSON document.
    """
    current = value
    for segment in path:
        if current is MISSING:
            return MISSING
        current = _maybe_parse_json(current)

        if isinstance(current, Mapping):
            if segment in current:
                current = current[segment]
                continue
            # JSON object keys are always strings, so an integer segment may still name one
            if isinstance(segment, int) and str(segment) in current:
                current = current[str(segment)]
                continue
            return MISSING

        if isinstance(current, Sequence) and not isinstance(current, (str, bytes)):
            if not isinstance(segment, int):
                return MISSING
            if -len(current) <= segment < len(current):
                current = current[segment]
                continue
            return MISSING

        return MISSING

    return current


def resolve(
    ip: IPReader,
    selector: Selector | str,
    separator: str = DEFAULT_PATH_SEPARATOR,
    content_type: str | None = None,
    attr_types: Mapping[str, str] | None = None,
) -> Any:
    """Resolve ``selector`` against ``ip``, returning ``MISSING`` when it does not apply.

    ``content_type`` is a fallback for IPs that arrive without ``sysAttributes.contentType``;
    ``attr_types`` maps attribute names to Cap'n Proto type strings, for attributes written without
    a ``valueType`` (see ``values.python_from_attr``).
    """
    if not isinstance(selector, Selector):
        selector = parse_selector(selector, separator)

    match selector.kind:
        case SelectorKind.LITERAL:
            return selector.literal

        case SelectorKind.META:
            if selector.name == META_TYPE:
                return str(ip.type)
            if selector.name == META_CONTENT_TYPE:
                resolved = content_type_of(ip)
                return resolved if resolved is not None else MISSING
            logger.debug("Unknown IP metadata selector %r; known names are %s.", selector.raw, sorted(_META_NAMES))
            return MISSING

        case SelectorKind.ATTRIBUTE:
            kv = attr_reader(ip, selector.name)
            if kv is None:
                return MISSING
            value = python_from_attr(kv, (attr_types or {}).get(selector.name))
            return apply_path(value, selector.path) if selector.path else value

        case SelectorKind.CONTENT:
            value = python_from_content(ip, content_type)
            return apply_path(value, selector.path) if selector.path else value

        case SelectorKind.GJSON:
            document = _maybe_parse_json(python_from_content(ip, content_type))
            if document is MISSING:
                return MISSING
            try:
                return gjson.get(document, selector.query)
            except gjson.GJSONError:
                logger.debug("GJSON query %r did not match.", selector.query)
                return MISSING

    return MISSING


class Op(StrEnum):
    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    MATCHES = "matches"
    EXISTS = "exists"
    THE_MISSING = "missing"
    IS_NULL = "is_null"
    TRUTHY = "truthy"


_ORDERING_OPS: Final[frozenset[Op]] = frozenset({Op.LT, Op.LE, Op.GT, Op.GE})
_UNARY_OPS: Final[frozenset[Op]] = frozenset({Op.EXISTS, Op.THE_MISSING, Op.IS_NULL, Op.TRUTHY})


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "yes", "1"):
            return True
        if lowered in ("false", "no", "0"):
            return False
    return None


def coerce_pair(left: Any, right: Any) -> tuple[Any, Any]:
    """Bring two values to a comparable pair, permissively (D8).

    Same-typed values are left alone. Otherwise numbers win over numeric strings, booleans over
    boolean-ish strings, and everything else falls back to string comparison.
    """
    if type(left) is type(right):
        return left, right
    if isinstance(left, bool) or isinstance(right, bool):
        left_bool, right_bool = _as_bool(left), _as_bool(right)
        if left_bool is not None and right_bool is not None:
            return left_bool, right_bool
    left_number, right_number = _as_number(left), _as_number(right)
    if left_number is not None and right_number is not None:
        return left_number, right_number
    if left is None or right is None:
        return left, right
    return str(left), str(right)


def compare(left: Any, right: Any, op: Op, strict_types: bool = False) -> bool:
    """Apply a binary operator, coercing operands unless ``strict_types``."""
    if not strict_types and op not in (Op.IN, Op.NOT_IN, Op.CONTAINS):
        left, right = coerce_pair(left, right)

    match op:
        case Op.EQ:
            return bool(left == right)
        case Op.NE:
            return bool(left != right)
        case Op.IN | Op.NOT_IN:
            container = right if isinstance(right, (list, tuple, set, frozenset, str, Mapping)) else [right]
            found = any(compare(left, item, Op.EQ, strict_types) for item in container)
            return found if op is Op.IN else not found
        case Op.CONTAINS:
            container = left if isinstance(left, (list, tuple, set, frozenset, str, Mapping)) else [left]
            return any(compare(item, right, Op.EQ, strict_types) for item in container)
        case Op.STARTSWITH:
            return str(left).startswith(str(right))
        case Op.ENDSWITH:
            return str(left).endswith(str(right))
        case Op.MATCHES:
            try:
                return re.search(str(right), str(left)) is not None
            except re.error:
                logger.warning("Invalid regular expression in 'matches' predicate: %r", right)
                return False

    if op in _ORDERING_OPS:
        try:
            match op:
                case Op.LT:
                    return bool(left < right)
                case Op.LE:
                    return bool(left <= right)
                case Op.GT:
                    return bool(left > right)
                case _:
                    return bool(left >= right)
        except TypeError:
            logger.debug("Cannot order %r against %r.", left, right)
            return False

    return False


class Predicate(BaseModel):
    """A leaf test (``left``/``op``/``right``) or a combinator (``all``/``any``/``not``).

    Written for TOML and JSON config, e.g.::

        [[predicates]]
        left = "@region"
        op = "eq"
        right = "brandenburg"
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    left: Any = None
    op: Op = Op.TRUTHY
    right: Any = None
    all_: list[Predicate] | None = Field(default=None, alias="all")
    any_: list[Predicate] | None = Field(default=None, alias="any")
    not_: Predicate | None = Field(default=None, alias="not")
    strict_types: bool = False

    @model_validator(mode="after")
    def exactly_one_form(self) -> Self:
        combinators = [name for name, value in (("all", self.all_), ("any", self.any_), ("not", self.not_)) if value]
        if len(combinators) > 1:
            msg = f"A predicate may use only one of 'all', 'any', 'not'; got {combinators}."
            raise ValueError(msg)
        if combinators and self.left is not None:
            msg = f"A predicate using '{combinators[0]}' must not also set 'left'."
            raise ValueError(msg)
        if not combinators and self.left is None and self.op not in _UNARY_OPS:
            msg = f"A leaf predicate needs 'left' (or a unary op); got op '{self.op}'."
            raise ValueError(msg)
        return self

    @property
    def is_combinator(self) -> bool:
        return bool(self.all_ or self.any_ or self.not_)


def evaluate(
    ip: IPReader,
    predicate: Predicate,
    separator: str = DEFAULT_PATH_SEPARATOR,
    content_type: str | None = None,
    attr_types: Mapping[str, str] | None = None,
) -> bool:
    """Evaluate ``predicate`` against ``ip``. An unresolvable selector makes a leaf ``False``."""
    kwargs = {"separator": separator, "content_type": content_type, "attr_types": attr_types}

    if predicate.all_:
        return all(evaluate(ip, child, **kwargs) for child in predicate.all_)
    if predicate.any_:
        return any(evaluate(ip, child, **kwargs) for child in predicate.any_)
    if predicate.not_:
        return not evaluate(ip, predicate.not_, **kwargs)

    left = resolve(ip, parse_selector(predicate.left, separator), **kwargs)

    match predicate.op:
        case Op.EXISTS:
            return left is not MISSING
        case Op.THE_MISSING:
            return left is MISSING
        case Op.IS_NULL:
            return left is None
        case Op.TRUTHY:
            return left is not MISSING and bool(left)

    if left is MISSING:
        return False

    right = resolve(ip, parse_selector(predicate.right, separator), **kwargs)
    if right is MISSING:
        return False

    return compare(left, right, predicate.op, predicate.strict_types)


Predicate.model_rebuild()
