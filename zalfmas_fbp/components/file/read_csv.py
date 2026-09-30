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

import csv
import logging
from pathlib import Path
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


class Config(process.ProcessConfig):
    file: str = Field(default="", description="Path to the CSV file to read.")
    struct_type: str = Field(
        default="@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup",
        description="Cap'n Proto struct type to build from each row.",
    )
    id_col: str = Field(default="id", description="Column identifying a row, used by 'send_ids'.")
    col_to_field_names: dict[str, str] = Field(
        default_factory=dict,
        description="Map CSV column names onto the struct's field names, e.g. {'col1': 'field1'}.",
    )
    send_ids: list[Any] = Field(
        default_factory=list,
        description="Emit only rows whose 'id_col' is in this list. Empty emits every row.",
    )
    to_attr: str | None = Field(
        default=None,
        description="Put the row in this attribute instead of the IP's content.",
    )
    delimiter: str | None = Field(
        default=None,
        description="Column delimiter. Null sniffs it from the file, trying ';', ',' and tab.",
    )
    wrap_in_substream: bool = Field(
        default=False,
        description="Wrap each file's rows in an open-/close-bracket pair.",
    )
    unknown_columns: Literal["ignore", "error"] = Field(
        default="ignore",
        description="Whether a column the struct has no field for is skipped or stops the file.",
    )


METADATA = meta.Component(
    category=meta.Category(id="file", name="File"),
    info=meta.Info(
        id="0e7507f8-97ae-4479-a608-4c1ebf37c4ba",
        name="read csv",
        description=(
            "Read a CSV file and emit each row as a typed Cap'n Proto struct. A source: it reads "
            "its file from config, and reads another whenever new config arrives on 'conf', so a "
            "flow can drive it over several files."
        ),
    ),
    type="process",
    outPorts=[
        meta.Port(
            name="out",
            contentType="AnyPointer",
            desc="One IP per row, tagged with the struct type it was built as.",
            required=True,
        ),
    ],
    config=Config,
)


class ReadCsv(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        super().__init__(metadata=metadata, con_man=con_man)
        self.rows_emitted: int = 0

    def rows_of(self, path: Path) -> list[dict[str, str]]:
        """The file's rows as dicts keyed by the struct's field names."""
        with path.open(newline="") as handle:
            if self.config.delimiter:
                reader = csv.reader(handle, delimiter=self.config.delimiter)
            else:
                sample = handle.read()
                handle.seek(0)
                reader = csv.reader(handle, csv.Sniffer().sniff(sample, delimiters=";,\t"))

            header = next(reader, None)
            if header is None:
                return []
            names = [self.config.col_to_field_names.get(col, col) for col in header]
            return [{name: value for name, value in zip(names, row, strict=False)} for row in reader]

    def _wanted(self, row: dict[str, str], id_field: str) -> bool:
        if not self.config.send_ids:
            return True
        value = row.get(id_field)
        return any(str(value) == str(wanted) for wanted in self.config.send_ids)

    async def _emit_file(self, schema: Any) -> bool:
        path = Path(self.config.file)
        if not path.is_file():
            logger.error("%s: no such file: %s", self.name, path)
            return True

        try:
            rows = self.rows_of(path)
        except (OSError, csv.Error, UnicodeDecodeError):
            logger.exception("%s: could not read %s", self.name, path)
            return True

        id_field = self.config.col_to_field_names.get(self.config.id_col, self.config.id_col)
        known = set(getattr(schema, "fieldnames", ()) or ())

        if self.config.wrap_in_substream and not await self.write_out(
            "out",
            brackets.make_bracket("openBracket", {"file": str(path)}),
        ):
            return False

        emitted = 0
        for row in rows:
            if not self._wanted(row, id_field):
                continue
            # Only the columns the struct actually has; capnp_from_json does the type coercion,
            # which is the same one json_to_capnp uses rather than a second copy here.
            fields = {name: value for name, value in row.items() if name in known}
            try:
                built = values.capnp_from_json(
                    fields,
                    schema,
                    unknown_fields=self.config.unknown_columns,
                    coerce_numbers=True,
                )
            except (TypeError, ValueError):
                logger.exception("%s: could not build %s from row %r", self.name, self.config.struct_type, row)
                continue

            out_ip = fbp_capnp.IP.new_message()
            if self.config.to_attr:
                brackets.set_attrs(out_ip, {self.config.to_attr: brackets.Attr(built, self.config.struct_type)})
            else:
                out_ip.content = built
                out_ip.sysAttributes.contentType = self.config.struct_type

            emitted += 1
            self.rows_emitted += 1
            if not await self.write_out("out", out_ip):
                return False

        logger.info("%s: emitted %d row(s) from %s", self.name, emitted, path)

        return not self.config.wrap_in_substream or await self.write_out(
            "out",
            brackets.make_bracket("closeBracket", {"substream_length": emitted}),
        )

    @override
    async def run(self):
        logger.info("%s process running", self.name)

        while self.out_ports["out"]:
            schema = values.resolve_schema(self.config.struct_type)
            if schema is None:
                logger.error("%s: could not resolve struct_type %r.", self.name, self.config.struct_type)
            elif not await self._emit_file(schema):
                break

            # A source reaches no read boundary, so it asks for the next config itself; this is
            # what lets one instance read a sequence of files.
            if not await self.next_config():
                break

        logger.info("%s process finished, emitted %d row(s)", self.name, self.rows_emitted)


def main():
    process.run_process_from_metadata_and_cmd_args(ReadCsv(METADATA), METADATA)


if __name__ == "__main__":
    main()
