#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */
"""Copyable template for a Process-based component.

Replace the placeholder metadata values, then adapt the typed config, ports, and `run()` logic.

It shows the four things every component has to get right, each a decision with a reason rather
than a style preference (see `agents_process.md`):

- **Bracket transparency.** Bracket IPs are forwarded unchanged, so a substream passing through
  stays one substream. Only a component that genuinely reasons about grouping does otherwise.
- **Attribute propagation.** `brackets.copy_attrs` preserves `desc` and `valueType`, applies every
  override rather than only the first, and wraps plain Python values into a `common.Value`.
- **No config reading.** The runtime owns the `conf` port - it applies the initial config before
  `run()` and later ones between IPs - so a component just reads `self.config`.
- **Error handling that does not hide bugs.** Guard only the step that can fail on *caller data*,
  and name what it can fail with. Do not wrap the whole per-IP body in `except Exception` and carry
  on: a component that fails on every IP then looks exactly like one with no input at all, which
  is the hardest kind of flow problem to find. Let a fault in the component itself stop the
  process - the runtime records it and reports the component as failed.
"""

from __future__ import annotations

import logging
from typing import Literal, override

import capnp
from mas.schema.fbp import fbp_capnp
from pydantic import Field
from zalfmas_common import common

from zalfmas_fbp.components.common import brackets
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process
from zalfmas_fbp.run.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class TemplateProcessConfig(process.ProcessConfig):
    """Example typed config model used as the single source of truth for metadata."""

    prefix: str = Field("", description="Optional prefix added to each outgoing message.")
    attribute_name: str | None = Field(
        "processedBy",
        description="If set, add this attribute to outgoing IPs with the component name as value.",
    )
    on_error: Literal["skip", "fail"] = Field(
        "skip",
        description="Whether an IP this component cannot read is skipped or stops the process.",
    )


METADATA = meta.Component(
    category=meta.Category(
        id="templates",
        name="Templates",
    ),
    info=meta.Info(
        id="replace-with-process-component-id",
        name="replace with process component name",
        description=(
            "Template process component to copy and adapt. Substream transparent: bracket IPs are forwarded unchanged."
        ),
    ),
    type="process",
    # 'conf' and 'log' are runtime-owned: do not declare them, they are injected.
    inPorts=[
        meta.Port(
            name="in",
            contentType="Text",
            desc="Incoming text messages.",
            required=True,
        ),
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text",
            desc="Outgoing text messages.",
            required=True,
        ),
    ],
    config=TemplateProcessConfig,
)


class TemplateProcessComponent(process.Process[TemplateProcessConfig]):
    """Minimal Process component showing config, IO, and attribute propagation."""

    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        """Read input IPs, transform content, preserve attrs, and emit output IPs."""

        logger.info("%s process running", self.name)

        while self.in_ports["in"] and self.out_ports["out"]:
            in_msg = await self.read_in("in")
            if in_msg is None:
                self.in_ports["in"] = None
                break

            # Forward the caller's grouping untouched; this component only transforms data.
            if brackets.is_bracket(in_msg):
                if not await self.write_out("out", in_msg):
                    logger.info("%s process finished", self.name)
                    return
                continue

            text = self._text_of(in_msg)
            if text is None:
                # Bad input from upstream is this component's business; a bug in the component is
                # not, and must not be turned into a silent skip.
                message = f"{self.name}: no text could be read from this IP"
                if self.config.on_error == "fail":
                    raise ValueError(message)
                logger.warning(message)
                continue

            out_ip = fbp_capnp.IP.new_message(content=f"{self.config.prefix}{text}")
            brackets.copy_attrs(in_msg, out_ip, extra=self._extra_attributes())
            if not await self.write_out("out", out_ip):
                logger.info("%s process finished", self.name)
                return

        logger.info("%s process finished", self.name)

    def _text_of(self, in_ip) -> str | None:
        """The IP's text, or None if it carries none.

        The guard is around exactly one step - reading this IP's payload - and names the one thing
        it can raise. Everything else in `run()` is left unguarded on purpose.
        """

        try:
            return in_ip.content.as_text()
        except capnp.KjException:
            return None

    def _extra_attributes(self) -> dict[str, str]:
        """Return example extra attributes to attach in addition to copied input attrs."""

        if self.config.attribute_name is None:
            return {}
        return {self.config.attribute_name: self.name}


def main():
    """Run the template component with the default Process CLI/bootstrap flow."""

    process.run_process_from_metadata_and_cmd_args(TemplateProcessComponent(METADATA), METADATA)


if __name__ == "__main__":
    main()
