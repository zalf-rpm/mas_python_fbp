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
from typing import Any

import gjson
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class ApplyGJsonQueriesConfig(process.ProcessConfig):
    queries: list[str] = Field(
        default_factory=list,
        description="GJSON queries applied sequentially. Query i+1 runs on the result of query i.",
    )
    merge_substream_results: bool = Field(
        False,
        description=(
            "If true, flatten first-level substreams by merging object results or concatenating array results, "
            "and emit one result message for each completed substream."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="json",
        name="JSON",
    ),
    info=meta.Info(
        id="b649bc12-4473-44bb-89b5-1de1201bf545",
        name="Apply GJSON queries",
        description="Apply sequential GJSON queries on JSON input and optionally merge first-level substream results.",
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
            desc="Input JSON text.",
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="Result JSON after applying all configured queries.",
        ),
    ],
    config=ApplyGJsonQueriesConfig,
)


def _apply_queries(value: Any, queries: list[str]) -> Any:
    result: Any = value
    for query in queries:
        result = gjson.get(result, query)
    return result


def _merge_substream_payloads(payloads: list[Any]) -> Any:
    if len(payloads) == 0:
        return None

    if all(isinstance(item, dict) for item in payloads):
        merged: dict[str, Any] = {}
        for item in payloads:
            merged.update(item)
        return merged

    if all(isinstance(item, list) for item in payloads):
        merged_list: list[Any] = []
        for item in payloads:
            merged_list.extend(item)
        return merged_list

    return payloads


class ApplyGJsonQueries(process.Process[ApplyGJsonQueriesConfig]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    async def _write_json_result(self, payload: Any, attrs: Any) -> bool:
        out_ip = fbp_capnp.IP.new_message(content=json.dumps(payload), attributes=attrs)
        return await self.write_out("out", out_ip)

    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        in_first_level_substream = False
        nested_level = 0
        substream_payloads: list[Any] = []
        substream_attrs = None

        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                if self.config.merge_substream_results and in_first_level_substream:
                    merged = _merge_substream_payloads(substream_payloads)
                    if not await self._write_json_result(merged, substream_attrs or []):
                        logger.info("%s process finished", self.name)
                        return
                break

            if in_msg.type == "openBracket":
                if self.config.merge_substream_results:
                    if not in_first_level_substream:
                        in_first_level_substream = True
                        nested_level = 1
                        substream_payloads = []
                        substream_attrs = None
                    else:
                        nested_level += 1
                else:
                    if not await self.write_out("out", in_msg):
                        logger.info("%s process finished", self.name)
                        return
                continue

            if in_msg.type == "closeBracket":
                if self.config.merge_substream_results:
                    if in_first_level_substream:
                        nested_level -= 1
                        if nested_level == 0:
                            merged = _merge_substream_payloads(substream_payloads)
                            if not await self._write_json_result(merged, substream_attrs or []):
                                logger.info("%s process finished", self.name)
                                return
                            in_first_level_substream = False
                            substream_payloads = []
                            substream_attrs = None
                else:
                    if not await self.write_out("out", in_msg):
                        logger.info("%s process finished", self.name)
                        return
                continue

            try:
                parsed = json.loads(in_msg.content.as_text())
                result = _apply_queries(parsed, self.config.queries)
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                logger.warning("%s failed to apply GJSON queries: %s", self.name, exc)
                continue

            if self.config.merge_substream_results and in_first_level_substream:
                if substream_attrs is None:
                    substream_attrs = in_msg.attributes
                substream_payloads.append(result)
                continue

            if not await self._write_json_result(result, in_msg.attributes):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(ApplyGJsonQueries(METADATA), METADATA)


if __name__ == "__main__":
    main()
