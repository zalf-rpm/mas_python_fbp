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
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()
_MISSING = object()
_ALL_GROUP = "__all__"


class InterpolateJsonByKeyConfig(process.ProcessConfig):
    traversal_path: str | None = Field(
        None,
        description="Optional path from input root to list leaf that should be processed.",
    )
    path_separator: str = Field(
        "/",
        description="Separator for traversal_path.",
    )
    group_key: str | None = Field(
        None,
        description="Optional key used to split list items into groups (e.g. TREAT_ID).",
    )
    x_key: str = Field(
        "x",
        description="Key in each list item used as x-axis value.",
    )
    y_key: str = Field(
        "y",
        description="Key in each list item used as y-axis value.",
    )
    inter_x: list[int | float | str] = Field(
        default_factory=list,
        description="Query x values at which interpolated y values are calculated.",
    )
    extrapolation: Literal["null", "clamp", "linear"] = Field(
        "null",
        description=(
            "How to handle inter_x outside the observed x-range: "
            "null -> y is null, clamp -> boundary y, linear -> linearly extrapolate."
        ),
    )
    on_error: Literal["keep_original", "null", "drop_message"] = Field(
        "null",
        description="Error handling per incoming message.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="e1018222-9029-4fbe-935d-cef5ab1c8011",
        name="Interpolate JSON by key",
        description="Group JSON objects and interpolate y=f(x) at configured x positions.",
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
            desc="Input JSON containing or being a list of objects.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="Output list of grouped interpolated points as JSON text.",
        ),
    ],
    config=InterpolateJsonByKeyConfig,
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


def _to_x_numeric(value: Any) -> tuple[float, Literal["number", "date"]]:
    if isinstance(value, bool):
        msg = "bool is not supported as x value"
        raise TypeError(msg)
    if isinstance(value, (int, float)):
        return float(value), "number"
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            return float(text), "number"
        except ValueError:
            pass

        try:
            return float(datetime.fromisoformat(text).date().toordinal()), "date"
        except ValueError:
            try:
                return float(date.fromisoformat(text).toordinal()), "date"
            except ValueError as exc:
                msg = f"Invalid x value '{value}'. Supported: number or ISO date."
                raise ValueError(msg) from exc

    msg = f"Unsupported x value type: {type(value).__name__}"
    raise TypeError(msg)


def _to_y_numeric(value: Any) -> float:
    if isinstance(value, bool):
        msg = "bool is not supported as y value"
        raise TypeError(msg)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    msg = f"Unsupported y value type: {type(value).__name__}"
    raise TypeError(msg)


def _compress_points(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    dedup: dict[float, float] = {}
    for x_val, y_val in points:
        dedup[x_val] = y_val
    return sorted(dedup.items(), key=lambda p: p[0])


def _linear_interpolate(
    points: list[tuple[float, float]],
    x_query: float,
    extrapolation: Literal["null", "clamp", "linear"],
) -> float | None:
    if len(points) == 0:
        return None

    if x_query <= points[0][0]:
        if x_query == points[0][0]:
            return points[0][1]
        if extrapolation == "null":
            return None
        if extrapolation == "clamp":
            return points[0][1]
        if len(points) < 2:
            return points[0][1]
        x0, y0 = points[0]
        x1, y1 = points[1]
        return y0 if x1 == x0 else y0 + ((y1 - y0) * (x_query - x0) / (x1 - x0))

    if x_query >= points[-1][0]:
        if x_query == points[-1][0]:
            return points[-1][1]
        if extrapolation == "null":
            return None
        if extrapolation == "clamp":
            return points[-1][1]
        if len(points) < 2:
            return points[-1][1]
        x0, y0 = points[-2]
        x1, y1 = points[-1]
        return y1 if x1 == x0 else y0 + ((y1 - y0) * (x_query - x0) / (x1 - x0))

    for i in range(1, len(points)):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        if x0 <= x_query <= x1:
            if x_query == x0:
                return y0
            if x_query == x1:
                return y1
            if x1 == x0:
                return y1
            return y0 + ((y1 - y0) * (x_query - x0) / (x1 - x0))

    return None


def _extract_leaf_as_list(data: Any, traversal_path: str | None, path_separator: str) -> list[Any]:
    leaf = _resolve_path(data, _split_path(traversal_path, path_separator)) if traversal_path else data
    if leaf is _MISSING:
        msg = f"Traversal path '{traversal_path}' could not be resolved."
        raise KeyError(msg)
    if not isinstance(leaf, list):
        msg = "Resolved leaf is not a list."
        raise TypeError(msg)
    return leaf


def _group_points(
    items: list[Any],
    group_key: str | None,
    x_key: str,
    y_key: str,
) -> tuple[dict[Any, list[tuple[float, float]]], dict[Any, Literal["number", "date"]]]:
    grouped_points: dict[Any, list[tuple[float, float]]] = {}
    group_x_types: dict[Any, Literal["number", "date"]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        if x_key not in item or y_key not in item:
            continue
        if item[x_key] is None or item[y_key] is None:
            continue

        if group_key is None:
            group_val = _ALL_GROUP
        elif group_key in item and item[group_key] is not None:
            group_val = item[group_key]
        else:
            continue

        x_num, x_type = _to_x_numeric(item[x_key])
        y_num = _to_y_numeric(item[y_key])

        if group_val in group_x_types and group_x_types[group_val] != x_type:
            msg = f"Mixed x value types in group '{group_val}'."
            raise ValueError(msg)
        group_x_types[group_val] = x_type
        grouped_points.setdefault(group_val, []).append((x_num, y_num))

    for group_val, points in grouped_points.items():
        grouped_points[group_val] = _compress_points(points)

    return grouped_points, group_x_types


def _interpolate_grouped(
    grouped_points: dict[Any, list[tuple[float, float]]],
    group_x_types: dict[Any, Literal["number", "date"]],
    inter_x: list[int | float | str],
    group_key: str | None,
    x_key: str,
    y_key: str,
    extrapolation: Literal["null", "clamp", "linear"],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    if len(grouped_points) == 0:
        return output

    for group_val, points in grouped_points.items():
        x_type = group_x_types[group_val]
        for x_query_raw in inter_x:
            x_query, query_type = _to_x_numeric(x_query_raw)
            if query_type != x_type:
                msg = f"inter_x type mismatch for group '{group_val}'. Expected {x_type}, got {query_type}."
                raise ValueError(msg)

            y_interp = _linear_interpolate(points, x_query, extrapolation)
            row: dict[str, Any] = {
                x_key: x_query_raw,
                y_key: y_interp,
            }
            if group_key is not None:
                row[group_key] = group_val
            output.append(row)
    return output


def _transform(data: Any, cfg: InterpolateJsonByKeyConfig) -> list[dict[str, Any]]:
    leaf_list = _extract_leaf_as_list(data, cfg.traversal_path, cfg.path_separator)
    grouped_points, group_x_types = _group_points(
        leaf_list,
        cfg.group_key,
        cfg.x_key,
        cfg.y_key,
    )
    return _interpolate_grouped(
        grouped_points,
        group_x_types,
        cfg.inter_x,
        cfg.group_key,
        cfg.x_key,
        cfg.y_key,
        cfg.extrapolation,
    )


class InterpolateJsonByKey(process.Process[InterpolateJsonByKeyConfig]):
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
                in_data = json.loads(in_msg.content.as_text())
            except json.JSONDecodeError as exc:
                logger.warning("%s received invalid JSON input: %s", self.name, exc)
                if self.config.on_error == "drop_message":
                    continue
                out_data = None
            else:
                try:
                    out_data = _transform(in_data, self.config)
                except (KeyError, TypeError, ValueError) as exc:
                    logger.warning("%s could not interpolate input JSON: %s", self.name, exc)
                    if self.config.on_error == "drop_message":
                        continue
                    out_data = None if self.config.on_error == "null" else in_data

            out_ip = fbp_capnp.IP.new_message(content=json.dumps(out_data), attributes=in_msg.attributes)
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(InterpolateJsonByKey(METADATA), METADATA)


if __name__ == "__main__":
    main()
