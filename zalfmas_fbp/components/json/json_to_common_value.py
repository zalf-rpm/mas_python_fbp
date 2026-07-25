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
from typing import Any, Literal

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging
from zalfmas_fbp.run.process.config.config_codec import config_value_from_python

logger = logging.getLogger(__name__)
configure_logging()

_MISSING = object()

_INT_RANGES: dict[str, tuple[int, int]] = {
    "i8": (-128, 127),
    "i16": (-32768, 32767),
    "i32": (-2147483648, 2147483647),
    "i64": (-9223372036854775808, 9223372036854775807),
    "ui8": (0, 255),
    "ui16": (0, 65535),
    "ui32": (0, 4294967295),
    "ui64": (0, 18446744073709551615),
}

_FLOAT_MAX: dict[str, float] = {
    "f32": 3.4028235e38,
    "f64": 1.7976931348623157e308,
}


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
            name="conf",
            contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]",
        ),
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


def _split_path(path: str, separator: str) -> list[str | int]:
    if separator == "":
        return [path]

    parts: list[str | int] = []
    for part in path.split(separator):
        if part == "":
            continue
        if part.lstrip("-").isdigit():
            parts.append(int(part))
        else:
            parts.append(part)
    return parts


def _resolve_path(value: Any, parts: list[str | int]) -> Any:
    current = value
    for part in parts:
        if isinstance(part, int):
            if isinstance(current, list) and -len(current) <= part < len(current):
                current = current[part]
                continue
            return _MISSING
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        return _MISSING
    return current


def _value_fields() -> set[str]:
    try:
        return set(common_capnp.Value.schema.fieldnames)
    except Exception:
        # Fallback to minimal known set used in this repository.
        return {"i64", "ui64", "f64", "b", "t", "li64", "lf64", "lb", "lt"}


def _is_json_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _float_fits(field: str, value: float) -> bool:
    if math.isnan(value) or math.isinf(value):
        return True
    max_abs = _FLOAT_MAX.get(field)
    if max_abs is None:
        return False
    return abs(value) <= max_abs


def _coerce_scalar_for_field(value: Any, field: str) -> Any:
    if field == "b":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "1"):
                return True
            if lowered in ("false", "0"):
                return False
        msg = f"Cannot coerce {value!r} to bool"
        raise TypeError(msg)

    if field == "t":
        if isinstance(value, str):
            return value
        msg = f"Cannot coerce {value!r} to text"
        raise TypeError(msg)

    if field in _INT_RANGES:
        if isinstance(value, bool):
            msg = "bool values are not accepted as integers"
            raise TypeError(msg)
        if isinstance(value, int):
            min_val, max_val = _INT_RANGES[field]
            if min_val <= value <= max_val:
                return value
            msg = f"Integer {value} does not fit in {field}"
            raise ValueError(msg)
        if isinstance(value, float) and value.is_integer():
            ivalue = int(value)
            min_val, max_val = _INT_RANGES[field]
            if min_val <= ivalue <= max_val:
                return ivalue
            msg = f"Integer {ivalue} does not fit in {field}"
            raise ValueError(msg)
        if isinstance(value, str):
            text = value.strip()
            try:
                ivalue = int(text, 10)
            except ValueError:
                try:
                    fvalue = float(text)
                except ValueError as exc:
                    msg = f"Cannot coerce {value!r} to integer type {field}"
                    raise TypeError(msg) from exc
                if not fvalue.is_integer():
                    msg = f"Cannot coerce {value!r} to integer type {field}"
                    raise TypeError(msg)
                ivalue = int(fvalue)
            min_val, max_val = _INT_RANGES[field]
            if min_val <= ivalue <= max_val:
                return ivalue
            msg = f"Integer {ivalue} does not fit in {field}"
            raise ValueError(msg)
        msg = f"Cannot coerce {value!r} to integer type {field}"
        raise TypeError(msg)

    if field in _FLOAT_MAX:
        if isinstance(value, bool):
            msg = "bool values are not accepted as float"
            raise TypeError(msg)
        if isinstance(value, (int, float)):
            fvalue = float(value)
            if _float_fits(field, fvalue):
                return fvalue
            msg = f"Float {fvalue} does not fit in {field}"
            raise ValueError(msg)
        if isinstance(value, str):
            try:
                fvalue = float(value.strip())
            except ValueError as exc:
                msg = f"Cannot coerce {value!r} to float type {field}"
                raise TypeError(msg) from exc
            if _float_fits(field, fvalue):
                return fvalue
            msg = f"Float {fvalue} does not fit in {field}"
            raise ValueError(msg)
        msg = f"Cannot coerce {value!r} to float type {field}"
        raise TypeError(msg)

    msg = f"Unsupported Value field: {field}"
    raise ValueError(msg)


def _list_field_for_scalar(field: str) -> str:
    if field == "b":
        return "lb"
    if field == "t":
        return "lt"
    return f"l{field}"


def _kind_of_scalar(value: Any) -> Literal["bool", "int", "float", "str"]:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    msg = f"Unsupported scalar JSON type: {type(value).__name__}"
    raise TypeError(msg)


def _find_numeric_field(kind: Literal["int", "float"], values: list[Any], fields: set[str], smallest: bool) -> str:
    if kind == "int":
        has_negative = any(v < 0 for v in values if isinstance(v, int))
        if has_negative:
            candidates = ["i8", "i16", "i32", "i64"] if smallest else ["i64", "i32", "i16", "i8"]
        else:
            candidates = (
                ["ui8", "ui16", "ui32", "ui64", "i8", "i16", "i32", "i64"]
                if smallest
                else [
                    "ui64",
                    "i64",
                    "ui32",
                    "i32",
                    "ui16",
                    "i16",
                    "ui8",
                    "i8",
                ]
            )
    else:
        candidates = ["f32", "f64"] if smallest else ["f64", "f32"]

    for candidate in candidates:
        if candidate not in fields:
            continue
        try:
            _ = [_coerce_scalar_for_field(v, candidate) for v in values]
            return candidate
        except (TypeError, ValueError):
            continue

    # Final stable fallback compatible with current codebase usage.
    return "f64" if kind == "float" else "i64"


def _determine_scalar_field(value: Any, fields: set[str], smallest: bool) -> str:
    kind = _kind_of_scalar(value)
    if kind == "bool":
        return "b" if "b" in fields else "i64"
    if kind == "str":
        return "t" if "t" in fields else "t"
    return _find_numeric_field(kind, [value], fields, smallest)


def _determine_list_field(values: list[Any], fields: set[str], smallest: bool) -> str:
    if len(values) == 0:
        for candidate in ["lf64", "li64", "lt", "lb"]:
            if candidate in fields:
                return candidate
        return "lf64"

    kinds = {_kind_of_scalar(v) for v in values}
    if kinds == {"bool"}:
        return "lb" if "lb" in fields else "li64"
    if kinds == {"str"}:
        return "lt" if "lt" in fields else "lt"

    int_candidates = (
        ["i8", "i16", "i32", "i64", "ui8", "ui16", "ui32", "ui64"]
        if smallest
        else [
            "ui64",
            "i64",
            "ui32",
            "i32",
            "ui16",
            "i16",
            "ui8",
            "i8",
        ]
    )
    float_candidates = ["f32", "f64"] if smallest else ["f64", "f32"]
    list_candidates: list[str] = []
    list_candidates.extend([_list_field_for_scalar(c) for c in int_candidates if _list_field_for_scalar(c) in fields])
    list_candidates.extend([_list_field_for_scalar(c) for c in float_candidates if _list_field_for_scalar(c) in fields])

    for candidate in list_candidates:
        try:
            _ = _coerce_list_for_field(values, candidate)
            return candidate
        except (TypeError, ValueError):
            continue

    msg = f"List contains mixed incompatible types: {kinds}"
    raise TypeError(msg)


def _coerce_list_for_field(values: list[Any], list_field: str) -> list[Any]:
    if list_field == "lb":
        return [_coerce_scalar_for_field(v, "b") for v in values]
    if list_field == "lt":
        return [_coerce_scalar_for_field(v, "t") for v in values]
    if not list_field.startswith("l"):
        msg = f"Field {list_field} is not a list field"
        raise ValueError(msg)
    scalar_field = list_field[1:]
    return [_coerce_scalar_for_field(v, scalar_field) for v in values]


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


def _create_value_message(field: str, value: Any) -> common_capnp.types.builders.ValueBuilder:
    return common_capnp.Value.new_message(**{field: value})


def _sentinel_value_for_selected_type(
    selected_type: str, sentinel_value: Any
) -> common_capnp.types.builders.ValueBuilder:
    scalar_type = selected_type[1:] if selected_type.startswith("l") else selected_type
    if scalar_type in {"v", "pair"}:
        return config_value_from_python(sentinel_value)
    coerced = _coerce_scalar_for_field(sentinel_value, scalar_type)
    return _create_value_message(scalar_type, coerced)


def _build_value_auto(
    value: Any,
    cfg: JsonToCommonValueConfig,
    fields: set[str],
    *,
    use_requested_type: bool,
) -> tuple[common_capnp.types.builders.ValueBuilder, str]:
    requested = cfg.requested_type if use_requested_type and cfg.requested_type != "auto" else None

    if isinstance(value, dict):
        if requested is not None and requested != "lpair":
            if not cfg.allow_fallback_if_requested_type_fails:
                msg = f"Requested type '{requested}' cannot hold object input (requires lpair)."
                raise ValueError(msg)
            logger.warning(
                "Requested type '%s' cannot hold object input; falling back to lpair.",
                requested,
            )
        if "lpair" not in fields:
            msg = "common_capnp.Value has no 'lpair' field in current schema."
            raise ValueError(msg)
        pairs = []
        for key, item in value.items():
            child_value, _ = _build_value_auto(item, cfg, fields, use_requested_type=False)
            pairs.append(common_capnp.Pair.new_message(fst=str(key), snd=child_value))
        return _create_value_message("lpair", pairs), "lpair"

    is_list = isinstance(value, list)
    if requested:
        if requested not in fields:
            if not cfg.allow_fallback_if_requested_type_fails:
                msg = f"Requested type '{requested}' is not available in common_capnp.Value schema."
                raise ValueError(msg)
            logger.warning("Requested type '%s' unavailable; falling back to fitting type.", requested)
        else:
            try:
                if is_list:
                    if requested == "lv":
                        lv_items = [_build_value_auto(item, cfg, fields, use_requested_type=False)[0] for item in value]
                        msg = _create_value_message("lv", lv_items)
                    elif not requested.startswith("l"):
                        msg = f"Requested scalar type '{requested}' cannot hold list input."
                        raise ValueError(msg)
                    else:
                        coerced_list = _coerce_list_for_field(value, requested)
                        msg = _create_value_message(requested, coerced_list)
                else:
                    if requested.startswith("l"):
                        err = f"Requested list type '{requested}' cannot hold scalar input."
                        raise ValueError(err)
                    coerced_scalar = _coerce_scalar_for_field(value, requested)
                    msg = _create_value_message(requested, coerced_scalar)
                return msg, requested
            except (TypeError, ValueError):
                if not cfg.allow_fallback_if_requested_type_fails:
                    raise
                logger.warning(
                    "Requested type '%s' cannot represent message values; falling back to fitting type.",
                    requested,
                )

    if not cfg.auto_select_type:
        msg = "No usable requested_type and auto_select_type is disabled."
        raise ValueError(msg)

    if is_list:
        try:
            selected_field = _determine_list_field(value, fields, cfg.optimize_smallest_type)
            if selected_field not in fields:
                msg = f"Selected list field '{selected_field}' not available in schema."
                raise ValueError(msg)
            coerced_list = _coerce_list_for_field(value, selected_field)
            return _create_value_message(selected_field, coerced_list), selected_field
        except TypeError:
            if "lv" not in fields:
                raise
            lv_items = [_build_value_auto(item, cfg, fields, use_requested_type=False)[0] for item in value]
            return _create_value_message("lv", lv_items), "lv"

    selected_field = _determine_scalar_field(value, fields, cfg.optimize_smallest_type)
    if selected_field not in fields:
        msg = f"Selected scalar field '{selected_field}' not available in schema."
        raise ValueError(msg)
    coerced_scalar = _coerce_scalar_for_field(value, selected_field)
    return _create_value_message(selected_field, coerced_scalar), selected_field


def _build_value(
    value: Any,
    cfg: JsonToCommonValueConfig,
    fields: set[str],
) -> tuple[common_capnp.types.builders.ValueBuilder, str, dict[str, common_capnp.types.builders.ValueBuilder]]:
    normalized, null_used, nan_used = _replace_sentinels_recursive(value, cfg.null_sentinel, cfg.nan_sentinel)
    value_msg, selected_field = _build_value_auto(normalized, cfg, fields, use_requested_type=True)
    sentinel_attrs: dict[str, common_capnp.types.builders.ValueBuilder] = {}
    if null_used:
        sentinel_attrs[cfg.null_sentinel_attr] = _sentinel_value_for_selected_type(selected_field, cfg.null_sentinel)
    if nan_used:
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
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        fields = _value_fields()

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
                        raise KeyError(msg)

                value_msg, _selected_type, sentinel_attrs = _build_value(payload, self.config, fields)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                logger.warning("%s could not convert JSON to common_capnp.Value: %s", self.name, exc)
                if self.config.skip_on_error:
                    continue
                value_msg = common_capnp.Value.new_message(t="")
                sentinel_attrs = {}

            out_ip = fbp_capnp.IP.new_message(content=value_msg)
            extra_attrs: dict[str, Any] = {}
            if self.config.attach_sentinel_attributes:
                extra_attrs.update(sentinel_attrs)
            common.copy_and_set_fbp_attrs(in_msg, out_ip, **extra_attrs)

            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(JsonToCommonValue(METADATA), METADATA)


if __name__ == "__main__":
    main()
