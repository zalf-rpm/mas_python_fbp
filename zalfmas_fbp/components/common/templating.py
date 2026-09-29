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
r"""Rendering a string from an IP, shared by ``string/format_string`` and ``file/write_file``.

A pattern is text with ``{...}`` placeholders. Each placeholder is a :mod:`selectors` selector, so
anything a predicate can test can also be interpolated::

    "{@region}_{count:03d}.csv"      -> "north_007.csv"
    "site {./site/id} on {now:%Y-%m-%d}"

Two names are reserved rather than being selectors: ``count`` (the IP's position in the stream, as
supplied by the caller) and ``now`` (render time, whose format spec is a strftime format). Anything
else must start with a selector sigil - ``@`` for an attribute, ``.`` for content, ``#`` for IP
metadata. A bare name is rejected rather than treated as a literal, so a typo is reported instead of
being rendered as itself.

``{{`` and ``}}`` are literal braces, as in :meth:`str.format`.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, Literal

from zalfmas_fbp.components.common import selectors, values

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

logger = logging.getLogger(__name__)

type MissingPolicy = Literal["error", "empty", "keep"]

COUNT_PLACEHOLDER: Final[str] = "count"
NOW_PLACEHOLDER: Final[str] = "now"
_SIGILS: Final[tuple[str, ...]] = ("@", ".", "#")

#: One placeholder: a name, optionally followed by ':' and a format spec. Doubled braces are
#: handled before this runs, so anything matching here is a real placeholder.
_PLACEHOLDER = re.compile(r"\{([^{}:]*)(?::([^{}]*))?\}")

_PLACEHOLDER_OPEN = "\x00"
_PLACEHOLDER_CLOSE = "\x01"


class TemplateError(ValueError):
    """A pattern is malformed, or a placeholder could not be rendered under 'error'."""


def placeholders_in(pattern: str) -> list[str]:
    """The placeholder names a pattern uses, in order, ignoring doubled braces."""
    masked = pattern.replace("{{", _PLACEHOLDER_OPEN).replace("}}", _PLACEHOLDER_CLOSE)
    return [match.group(1) for match in _PLACEHOLDER.finditer(masked)]


def validate_pattern(pattern: str) -> None:
    """Raise :class:`TemplateError` for a placeholder that is neither reserved nor a selector."""
    for name in placeholders_in(pattern):
        if name in (COUNT_PLACEHOLDER, NOW_PLACEHOLDER):
            continue
        if not name.startswith(_SIGILS):
            msg = (
                f"'{{{name}}}' is not a usable placeholder: use '{{@attr}}', '{{.}}' or '{{./path}}', "
                f"'{{#type}}', '{{{COUNT_PLACEHOLDER}}}' or '{{{NOW_PLACEHOLDER}}}'"
            )
            raise TemplateError(msg)


def _format_value(value: Any, spec: str | None, number_format: str | None) -> str:
    if spec:
        return format(value, spec)
    if number_format and isinstance(value, (int, float)) and not isinstance(value, bool):
        return format(value, number_format)
    return str(value)


def render(
    pattern: str,
    ip: IPReader | None = None,
    count: int | None = None,
    separator: str = "/",
    content_type: str | None = None,
    attr_types: Mapping[str, str] | None = None,
    missing: MissingPolicy = "empty",
    number_format: str | None = None,
    now: datetime | None = None,
) -> str:
    """Render ``pattern`` against ``ip``.

    ``missing`` decides what an unresolvable placeholder does: raise, render as empty, or stay in
    the output as it was written.
    """
    validate_pattern(pattern)
    timestamp = now or datetime.now().astimezone()
    masked = pattern.replace("{{", _PLACEHOLDER_OPEN).replace("}}", _PLACEHOLDER_CLOSE)

    def replace(match: re.Match[str]) -> str:
        name, spec = match.group(1), match.group(2)

        if name == COUNT_PLACEHOLDER:
            if count is None:
                return _on_missing(match, name, missing)
            return _format_value(count, spec, number_format)

        if name == NOW_PLACEHOLDER:
            return timestamp.strftime(spec) if spec else timestamp.isoformat()

        if ip is None:
            return _on_missing(match, name, missing)

        resolved = selectors.resolve(
            ip,
            name,
            separator=separator,
            content_type=content_type,
            attr_types=attr_types,
        )
        if resolved is values.MISSING or resolved is None:
            return _on_missing(match, name, missing)
        return _format_value(resolved, spec, number_format)

    rendered = _PLACEHOLDER.sub(replace, masked)
    return rendered.replace(_PLACEHOLDER_OPEN, "{").replace(_PLACEHOLDER_CLOSE, "}")


def _on_missing(match: re.Match[str], name: str, missing: MissingPolicy) -> str:
    if missing == "error":
        msg = f"'{{{name}}}' could not be resolved for this IP"
        raise TemplateError(msg)
    if missing == "keep":
        return match.group(0)
    return ""
