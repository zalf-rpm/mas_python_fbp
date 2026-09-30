#!/usr/bin/python
# -*- coding: UTF-8

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
from __future__ import annotations

import json
import logging
import math
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, selectors, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.process.config.config_codec import config_value_from_python

if TYPE_CHECKING:
    from mas.schema.common.common_capnp.types.builders import ValueBuilder

logger = logging.getLogger(__name__)
configure_logging()

_MISSING = object()


class JsonToCommonValueConfig(process.ProcessConfig):
    traversal_path: str | None = Field(
        None,
        description="Optional path from input root to the JSON value to convert.",
    )
    path_separator: str = Field(
        "/",
        description="Separator for traversal_path.",
    )
    requested_type: str | None = Field(
        None,
        description=(
            "Optional Value union field to force, e.g. lpair, lf64, li64, lt, lb, f64, i64, t, b. "
            "Set to null for automatic type selection."
        ),
    )
    auto_select_type: bool = Field(
        True,
        description="Automatically pick a fitting Value type from the JSON input.",
    )
    optimize_smallest_type: bool = Field(
        True,
        description="When auto selecting, choose the smallest fitting numeric type if possible.",
    )
    allow_fallback_if_requested_type_fails: bool = Field(
        True,
        description=(
            "If requested_type is set but values do not fit, fallback to a fitting type (if possible). "
            "If false, skip the message."
        ),
    )
    null_sentinel: int | float | bool | str | None = Field(
        None,
        description="Replacement value for JSON null in scalar/list conversions.",
    )
    nan_sentinel: int | float | bool | str | None = Field(
        None,
        description="Replacement value for NaN entries in float scalar/list conversions.",
    )
    attach_sentinel_attributes: bool = Field(
        True,
        description="Attach attributes describing sentinel values applied to outgoing messages.",
    )
    null_sentinel_attr: str = Field(
        "null_sentinel",
        description="Attribute name for applied null sentinel value.",
    )
    nan_sentinel_attr: str = Field(
        "nan_sentinel",
        description="Attribute name for applied NaN sentinel value.",
    )
    skip_on_error: bool = Field(
        True,
        description="Skip message on conversion errors.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="48701153-3743-4f42-a30d-e133065cbf4d",
        name="JSON to common Value",
        description="Convert JSON text into a common.capnp:Value with optional automatic type optimization.",
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text (JSON)",
            desc="JSON text to convert to common.capnp:Value.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="@0xe17592335373b246 = common/common.capnp:Value",
            desc="Converted Value.",
        ),
    ],
    config=JsonToCommonValueConfig,
)


def _split_path(path: str, separator: str) -> tuple[str | int, ...]:
    return selectors.split_path(path, separator)


def _resolve_path(value: Any, parts: Sequence[str | int]) -> Any:
    resolved = selectors.apply_path(value, parts)
    return _MISSING if resolved is values.MISSING else resolved


def _is_json_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _replace_sentinels_recursive(
    value: Any,
    null_sentinel: Any,
    nan_sentinel: Any,
) -> tuple[Any, bool, bool]:
    null_used = False
    nan_used = False

    def replace(v: Any) -> Any:
        nonlocal null_used, nan_used
        if isinstance(v, dict):
            return {str(k): replace(item) for k, item in v.items()}
        if isinstance(v, list):
            return [replace(item) for item in v]
        if v is None:
            if null_sentinel is None:
                msg = "JSON null encountered but null_sentinel is not configured"
                raise ValueError(msg)
            null_used = True
            return null_sentinel
        if _is_json_nan(v):
            if nan_sentinel is not None:
                nan_used = True
                return nan_sentinel
            # Keep NaN if target type supports float.
            return v
        return v

    return replace(value), null_used, nan_used


def _sentinel_value_for_selected_type(selected_type: str, sentinel_value: Any) -> ValueBuilder:
    """Type the sentinel attribute like the payload, so consumers can compare it like for like.

    The payload's type already accommodates the sentinel (see ``_build_value``), so this normally
    fits. The fallback is for the one case that cannot be accommodated: a scalar payload of one kind
    with a sentinel of another - text content with a numeric sentinel, say - where no single Value
    field can hold both. The attribute is worth more than the type match; failing here would drop
    the whole message.
    """
    scalar_type = selected_type[1:] if selected_type.startswith("l") else selected_type
    if scalar_type in {"v", "pair"}:
        return config_value_from_python(sentinel_value)
    try:
        coerced = values.coerce_scalar_for_field(sentinel_value, scalar_type)
    except (TypeError, ValueError):
        logger.debug(
            "Sentinel %r does not fit the payload's %r field; typing it on its own.",
            sentinel_value,
            scalar_type,
        )
        return values.value_from_python(sentinel_value)
    return values.value_message(scalar_type, coerced)


def _build_value(
    value: Any,
    cfg: JsonToCommonValueConfig,
) -> tuple[ValueBuilder, str, dict[str, ValueBuilder]]:
    normalized, null_used, nan_used = _replace_sentinels_recursive(value, cfg.null_sentinel, cfg.nan_sentinel)
    value_msg = values.value_from_python(
        normalized,
        requested_type=cfg.requested_type,
        auto_select=cfg.auto_select_type,
        smallest=cfg.optimize_smallest_type,
        allow_fallback=cfg.allow_fallback_if_requested_type_fails,
        # A configured sentinel belongs to the value domain whether or not this particular message
        # contains a null, so the chosen type must hold it. Otherwise a stream would emit lui8 for
        # the messages without nulls and li16 for the ones with them, and a consumer switching on
        # the union field would see the type flap message to message.
        must_accommodate=[s for s in (cfg.null_sentinel, cfg.nan_sentinel) if s is not None],
    )
    selected_field = value_msg.as_reader().which()

    sentinel_attrs: dict[str, ValueBuilder] = {}
    if null_used or cfg.null_sentinel is not None:
        sentinel_attrs[cfg.null_sentinel_attr] = _sentinel_value_for_selected_type(selected_field, cfg.null_sentinel)
    if nan_used or cfg.nan_sentinel is not None:
        sentinel_attrs[cfg.nan_sentinel_attr] = _sentinel_value_for_selected_type(selected_field, cfg.nan_sentinel)
    return value_msg, selected_field, sentinel_attrs


class JsonToCommonValue(process.Process[JsonToCommonValueConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    async def run(self):
        logger.info("%s process running", self.name)

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            if in_msg.type in ("openBracket", "closeBracket"):
                if not await self.write_out("out", in_msg):
                    logger.info("%s process finished", self.name)
                    return
                continue

            try:
                payload = json.loads(in_msg.content.as_text())
                if self.config.traversal_path:
                    payload = _resolve_path(
                        payload, _split_path(self.config.traversal_path, self.config.path_separator)
                    )
                    if payload is _MISSING:
                        msg = f"Could not resolve traversal_path '{self.config.traversal_path}'."
                        # raised to be caught just below, so an unresolvable path takes the same
                        # route as malformed JSON rather than needing a second error path
                        raise KeyError(msg)  # noqa: TRY301

                value_msg, _selected_type, sentinel_attrs = _build_value(payload, self.config)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                logger.warning("%s could not convert JSON to common_capnp.Value: %s", self.name, exc)
                if self.config.skip_on_error:
                    continue
                value_msg = common_capnp.Value.new_message(t="")
                sentinel_attrs = {}

            out_ip = fbp_capnp.IP.new_message(content=value_msg)
            extra_attrs: dict[str, Any] = {}
            if self.config.attach_sentinel_attributes:
                # already common.Value messages, so they are written as-is but tagged, which is
                # what lets a downstream component read them back (D4)
                extra_attrs.update(
                    {name: brackets.Attr(value, values.VALUE_TYPE) for name, value in sentinel_attrs.items()}
                )
            brackets.copy_attrs(in_msg, out_ip, extra=extra_attrs)

            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(JsonToCommonValue(METADATA), METADATA)


if __name__ == "__main__":
    main()
