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
from datetime import date, timedelta
from typing import Any, Literal, override

from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets, values
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

MAX_ELEMENTS = 1_000_000


class Config(process.ProcessConfig):
    mode: Literal["range", "list", "dates"] = Field(
        "range",
        description="Where the elements come from: a numeric range, a literal list, or a range of dates.",
    )
    start: float = Field(0, description="First value of a numeric range.")
    stop: float = Field(10, description="End of a numeric range, exclusive.")
    step: float = Field(1, description="Increment of a numeric range. May be negative, but not zero.")
    integers: bool = Field(
        True,
        description="Emit a numeric range as integers when start, stop and step are whole numbers.",
    )
    date_start: str | None = Field(None, description="First date of a date range, as ISO 8601 (YYYY-MM-DD).")
    date_stop: str | None = Field(None, description="End of a date range, exclusive, as ISO 8601.")
    date_step_days: int = Field(1, description="Increment of a date range in days. May be negative, but not zero.")
    date_format: str = Field("%Y-%m-%d", description="strftime format for emitted dates.")
    sequence_values: list[Any] = Field(
        default_factory=list,
        description="The elements to emit in 'list' mode, in order.",
    )
    repeat: int = Field(1, description="How many times to emit the whole sequence.")
    wrap_in_substream: bool = Field(
        False,
        description="Wrap each emission of the sequence in an open-/close-bracket pair.",
    )
    as_type: Literal["value", "json", "text"] = Field(
        "value",
        description=(
            "How to encode each element: 'value' as a common.capnp:Value, 'json' as JSON text, "
            "'text' as a plain string."
        ),
    )
    index_attr: str | None = Field(
        None,
        description="If set, attach the element's 0-based index within the emission as this attribute.",
    )
    emit: Literal["all_at_once", "on_trigger", "one_per_trigger"] = Field(
        "all_at_once",
        description=(
            "'all_at_once' emits the sequence immediately and finishes. 'on_trigger' emits the whole "
            "sequence for each IP received on 'trigger'. 'one_per_trigger' emits one element per "
            "trigger IP and finishes when the sequence runs out."
        ),
    )


METADATA = meta.Component(
    category=meta.Category(
        id="simple",
        name="Simple",
    ),
    info=meta.Info(
        id="feec45a3-4908-4dfe-b6ab-a56be97d8f77",
        name="Sequence",
        description=(
            "Generate a bounded sequence of numbers, dates or literal values. The counterpart to "
            "'counter', which is unbounded and integer only."
        ),
    ),
    type="process",
    inPorts=[
        meta.Port(
            name="trigger",
            contentType="AnyPointer",
            desc=(
                "Optional. Received IPs are discarded; they only pace emission, see the 'emit' "
                "config. Unconnected means the sequence is emitted immediately."
            ),
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="The sequence elements, encoded according to 'as_type'.",
        ),
    ],
    config=Config,
)


def _numeric_elements(cfg: Config) -> list[Any]:
    # Coerce explicitly: pydantic does not validate field defaults, so an untouched start=0 stays
    # an int while a configured step=0.5 is a float, which would emit mixed element types.
    start, stop, step = float(cfg.start), float(cfg.stop), float(cfg.step)
    if step == 0:
        msg = "'step' must not be zero"
        raise ValueError(msg)

    whole = cfg.integers and all(value.is_integer() for value in (start, stop, step))
    elements: list[Any] = []
    current = start
    while (current < stop) if step > 0 else (current > stop):
        elements.append(int(current) if whole else current)
        current += step
        if len(elements) > MAX_ELEMENTS:
            msg = f"numeric range exceeds {MAX_ELEMENTS} elements; check 'start', 'stop' and 'step'"
            raise ValueError(msg)
    return elements


def _date_elements(cfg: Config) -> list[str]:
    if cfg.date_start is None or cfg.date_stop is None:
        msg = "'dates' mode needs both 'date_start' and 'date_stop'"
        raise ValueError(msg)
    if cfg.date_step_days == 0:
        msg = "'date_step_days' must not be zero"
        raise ValueError(msg)

    start, stop = date.fromisoformat(cfg.date_start), date.fromisoformat(cfg.date_stop)
    step = timedelta(days=cfg.date_step_days)
    elements: list[str] = []
    current = start
    while (current < stop) if cfg.date_step_days > 0 else (current > stop):
        elements.append(current.strftime(cfg.date_format))
        current += step
        if len(elements) > MAX_ELEMENTS:
            msg = f"date range exceeds {MAX_ELEMENTS} elements; check 'date_step_days'"
            raise ValueError(msg)
    return elements


def elements_for(cfg: Config) -> list[Any]:
    """The sequence a config describes, as plain Python values."""
    if cfg.mode == "list":
        return list(cfg.sequence_values)
    if cfg.mode == "dates":
        return _date_elements(cfg)
    return _numeric_elements(cfg)


class Sequence(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    def _content_for(self, element: Any) -> Any:
        if self.config.as_type == "json":
            return json.dumps(element)
        if self.config.as_type == "text":
            return element if isinstance(element, str) else json.dumps(element)
        return values.value_from_python(element)

    def _ip_for(self, element: Any, index: int) -> Any:
        out_ip = fbp_capnp.IP.new_message(content=self._content_for(element))
        if self.config.index_attr:
            brackets.set_attrs(out_ip, {self.config.index_attr: index})
        return out_ip

    async def _emit(self, elements: list[Any]) -> bool:
        """Emit one pass over the sequence. Returns False once the output port is gone."""
        if self.config.wrap_in_substream and not await self.write_out("out", brackets.make_bracket("openBracket")):
            return False
        for index, element in enumerate(elements):
            if not await self.write_out("out", self._ip_for(element, index)):
                return False
        return not self.config.wrap_in_substream or await self.write_out(
            "out",
            brackets.make_bracket("closeBracket"),
        )

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        try:
            elements = elements_for(self.config)
        except (ValueError, TypeError):
            logger.exception("%s: could not build the sequence", self.name)
            return

        logger.info("%s: sequence has %d element(s)", self.name, len(elements))

        emit = self.config.emit
        if emit != "all_at_once" and not self.in_ports["trigger"]:
            logger.warning(
                "%s: 'emit' is %r but the 'trigger' port is not connected; emitting once immediately.",
                self.name,
                emit,
            )
            emit = "all_at_once"

        if emit == "all_at_once":
            for _ in range(max(1, self.config.repeat)):
                if not await self._emit(elements):
                    break
        elif emit == "on_trigger":
            await self._run_on_trigger(elements)
        else:
            await self._run_one_per_trigger(elements)

        logger.info("%s process finished", self.name)

    async def _run_on_trigger(self, elements: list[Any]) -> None:
        while self.in_ports["trigger"] and self.out_ports["out"]:
            if await self.read_in("trigger") is None:
                self.in_ports["trigger"] = None
                break
            for _ in range(max(1, self.config.repeat)):
                if not await self._emit(elements):
                    return

    async def _run_one_per_trigger(self, elements: list[Any]) -> None:
        passes = max(1, self.config.repeat)
        if self.config.wrap_in_substream and not await self.write_out("out", brackets.make_bracket("openBracket")):
            return

        for pass_index in range(passes):
            for index, element in enumerate(elements):
                if not self.in_ports["trigger"] or not self.out_ports["out"]:
                    return
                if await self.read_in("trigger") is None:
                    self.in_ports["trigger"] = None
                    logger.info(
                        "%s: 'trigger' closed after %d of %d element(s)",
                        self.name,
                        pass_index * len(elements) + index,
                        passes * len(elements),
                    )
                    break
                if not await self.write_out("out", self._ip_for(element, index)):
                    return
            else:
                continue
            break

        if self.config.wrap_in_substream:
            _ = await self.write_out("out", brackets.make_bracket("closeBracket"))


def main():
    process.run_process_from_metadata_and_cmd_args(Sequence(METADATA), METADATA)


if __name__ == "__main__":
    main()
