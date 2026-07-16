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
from datetime import date, datetime
from typing import Any, Literal

from mas.schema.fbp import fbp_capnp
from pydantic import BaseModel, Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class TransformOperation(BaseModel):
    op: Literal[
        "round",
        "int",
        "float",
        "string",
        "add",
        "sub",
        "mul",
        "div",
        "pow",
        "date_year",
        "date_month",
        "date_day",
        "date_day_of_year",
        "date_iso",
    ] = Field(description="Transformation operation to apply.")
    path: str | None = Field(
        None,
        description="Optional path in current mapped item to transform. If omitted, transform item itself.",
    )
    value: int | float | str | None = Field(
        None,
        description="Operand used by numeric operations (add/sub/mul/div/pow).",
    )
    ndigits: int | None = Field(
        None,
        description="Decimal digits used by round operation.",
    )


class MapJsonValuesConfig(process.ProcessConfig):
    path_separator: str = Field(
        "/",
        description="Separator used in operation paths.",
    )
    operations: list[TransformOperation] = Field(
        default_factory=list,
        description=(
            "Ordered transformation operations applied to each list item, object value, or the top-level atomic input."
        ),
    )
    max_abs_exponent: int = Field(
        100,
        description="Absolute exponent limit for pow operation.",
    )
    on_error: Literal["keep_original", "null", "drop_message"] = Field(
        "keep_original",
        description=(
            "Error behavior for a message: keep_original forwards unmodified input, "
            "null forwards JSON null, drop_message skips output."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="308d0b88-18f5-4d40-8b3f-bab35fdad44e",
        name="Map JSON values",
        description="Map over JSON list items or object values and apply configured transformation operations.",
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
            desc="Input JSON as list, object, or atomic value.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="Transformed JSON.",
        ),
    ],
    config=MapJsonValuesConfig,
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


def _get_list_item(value: list[Any], index: int) -> Any:
    if -len(value) <= index < len(value):
        return value[index]
    msg = f"list index {index} out of bounds"
    raise IndexError(msg)


def _set_list_item(value: list[Any], index: int, item: Any) -> None:
    if -len(value) <= index < len(value):
        value[index] = item
        return
    msg = f"list index {index} out of bounds"
    raise IndexError(msg)


def _get_at_path(value: Any, parts: list[str | int]) -> Any:
    current = value
    for part in parts:
        if isinstance(part, int):
            if not isinstance(current, list):
                msg = f"Expected list at path segment '{part}', got {type(current).__name__}"
                raise TypeError(msg)
            current = _get_list_item(current, part)
            continue

        if not isinstance(current, dict) or part not in current:
            msg = f"Path segment '{part}' not found"
            raise KeyError(msg)
        current = current[part]
    return current


def _set_at_path(value: Any, parts: list[str | int], new_value: Any) -> Any:
    if len(parts) == 0:
        return new_value

    parent = _get_at_path(value, parts[:-1]) if len(parts) > 1 else value
    last = parts[-1]
    if isinstance(last, int):
        if not isinstance(parent, list):
            msg = f"Expected list at path segment '{last}', got {type(parent).__name__}"
            raise TypeError(msg)
        _set_list_item(parent, last, new_value)
    else:
        if not isinstance(parent, dict):
            msg = f"Expected object for key '{last}', got {type(parent).__name__}"
            raise TypeError(msg)
        if last not in parent:
            msg = f"Path segment '{last}' not found"
            raise KeyError(msg)
        parent[last] = new_value
    return value


def _to_number(value: Any) -> float:
    if isinstance(value, bool):
        msg = "bool is not supported as number input"
        raise TypeError(msg)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    msg = f"Cannot convert {type(value).__name__} to number"
    raise TypeError(msg)


def _to_iso_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        msg = f"Cannot parse non-string {type(value).__name__} as ISO date"
        raise TypeError(msg)

    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            msg = f"Invalid ISO date value: {value}"
            raise ValueError(msg) from exc


def _require_operand(op_name: str, value: int | float | str | None) -> float:
    if value is None:
        msg = f"Operation '{op_name}' requires an operand in 'value'."
        raise ValueError(msg)
    return _to_number(value)


def _apply_scalar_op(value: Any, operation: TransformOperation, max_abs_exponent: int) -> Any:
    if value is None:
        return None

    op = operation.op
    if op == "round":
        number = _to_number(value)
        return round(number, operation.ndigits) if operation.ndigits is not None else round(number)
    if op == "int":
        return int(_to_number(value))
    if op == "float":
        return float(_to_number(value))
    if op == "string":
        return str(value)

    if op == "add":
        return _to_number(value) + _require_operand(op, operation.value)
    if op == "sub":
        return _to_number(value) - _require_operand(op, operation.value)
    if op == "mul":
        return _to_number(value) * _require_operand(op, operation.value)
    if op == "div":
        operand = _require_operand(op, operation.value)
        if operand == 0:
            msg = "Division by zero is not allowed."
            raise ZeroDivisionError(msg)
        return _to_number(value) / operand
    if op == "pow":
        exponent = _require_operand(op, operation.value)
        if abs(exponent) > max_abs_exponent:
            msg = f"Exponent {exponent} exceeds max_abs_exponent={max_abs_exponent}."
            raise ValueError(msg)
        return _to_number(value) ** exponent

    as_date = _to_iso_date(value)
    if op == "date_year":
        return as_date.year
    if op == "date_month":
        return as_date.month
    if op == "date_day":
        return as_date.day
    if op == "date_day_of_year":
        return as_date.timetuple().tm_yday
    if op == "date_iso":
        return as_date.isoformat()

    msg = f"Unsupported operation: {op}"
    raise ValueError(msg)


def _apply_operations_to_item(
    item: Any,
    operations: list[TransformOperation],
    path_separator: str,
    max_abs_exponent: int,
) -> Any:
    current_item = item
    for operation in operations:
        if operation.path is None or operation.path == "":
            current_item = _apply_scalar_op(current_item, operation, max_abs_exponent)
            continue

        path_parts = _split_path(operation.path, path_separator)
        value_at_path = _get_at_path(current_item, path_parts)
        transformed_value = _apply_scalar_op(value_at_path, operation, max_abs_exponent)
        current_item = _set_at_path(current_item, path_parts, transformed_value)
    return current_item


def _map_json(
    data: Any,
    operations: list[TransformOperation],
    path_separator: str,
    max_abs_exponent: int,
) -> Any:
    if isinstance(data, list):
        return [_apply_operations_to_item(item, operations, path_separator, max_abs_exponent) for item in data]
    if isinstance(data, dict):
        return {
            key: _apply_operations_to_item(value, operations, path_separator, max_abs_exponent)
            for key, value in data.items()
        }
    return _apply_operations_to_item(data, operations, path_separator, max_abs_exponent)


class MapJsonValues(process.Process[MapJsonValuesConfig]):
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
                input_data = json.loads(in_msg.content.as_text())
            except json.JSONDecodeError as exc:
                logger.warning("%s received invalid JSON input: %s", self.name, exc)
                if self.config.on_error == "drop_message":
                    continue
                transformed = None
            else:
                try:
                    transformed = _map_json(
                        input_data,
                        self.config.operations,
                        self.config.path_separator,
                        self.config.max_abs_exponent,
                    )
                except (TypeError, ValueError, KeyError, IndexError, ZeroDivisionError) as exc:
                    logger.warning("%s failed to transform input JSON: %s", self.name, exc)
                    if self.config.on_error == "drop_message":
                        continue
                    transformed = None if self.config.on_error == "null" else input_data

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(transformed), attributes=in_msg.attributes)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(MapJsonValues(METADATA), METADATA)


if __name__ == "__main__":
    main()
