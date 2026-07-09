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
from collections import defaultdict
from typing import Any, Literal, override

import capnp
import numpy as np
import pandas
from pydantic import Field
from zalfmas_capnp_schemas_with_stubs import (
    climate_capnp,
    common_capnp,
    fbp_capnp,
    field_exp_data_capnp,
    soil_capnp,
)
from zalfmas_common import common
from zalfmas_common.climate import csv_file_based
from zalfmas_services.soil import sqlite_soil_data_service as sds

import zalfmas_fbp.run.process as process
from zalfmas_fbp.run import metadata as meta

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s @ %(name)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Config(process.ProcessConfig):
    file: str = Field(
        "/path/to/excel/file.xls(x)",
        description="Path to Excel file (xls(x))",
    )
    sheets_to_load: list[str] = Field(
        [],
        description=(
            "Names of sheets to read dynamically. Their columns are read "
            "generically and the resulting sub-object is attached based on the key columns present in the "
            "sheet: PLTID(+TREAT_ID+EID) -> plot, TREAT_ID(+EID) -> treatment, EID -> experiment. The "
            "sub-object key is the lower-cased sheet name. If the key columns are unique per row a single "
            "object is stored, otherwise a list of rows is stored."
        ),
    )
    load_all: bool = Field(
        False,
        description="If True, automatically reads every sheet.",
    )
    index_of_header_row: int = Field(
        0, description="The row number (starting at 0), which should be used for the header names."
    )
    nesting_by_key_cols: dict[str, list[str]] = Field(
        {},
        description="""When set, specifies which nesting levels to create according to these keys in the header columns.
        E.g. {
            "experiments": ["EID"],
            "treatments": ["EID", "TREAT_ID"],
            "plots": ["EID", "TREAT_ID", "PLTID"],
        }
        This will create nested dictionaries if columns with these keys exist.""",
    )
    send_nesting_level: list[str] = Field(
        [], description="Send messages at this nesting level, e.g. ['EID', 'TREAT_ID']."
    )
    include_branch_level_data: bool = Field(
        False,
        description="""If send nesting is not empty, then if set to true sends the data at the branch level, before
        traversing down the nesting level as extra message.
        If there is no nested sending requested, just one message with all data will be send.""",
    )
    include_brackets_when_sending_nested: bool = Field(
        True, description="""Add a bracket IP pair around each nesting level."""
    )


METADATA = meta.Component(
    category=meta.Category(
        id="file",
        name="File",
    ),
    info=meta.Info(
        id="361d2bb9-504e-4173-9d58-26c71d50d6b3",
        name="Read Excel file",
        description="Turn selected excel tables into JSON datastructures",
    ),
    type="process",
    inPorts=[
        meta.Port(name="conf", contentType="@0xed6c098b67cad454 = common/common.capnp:StructuredText[JSON | TOML]")
    ],
    outPorts=[
        meta.Port(
            name="out",
            contentType="Text (JSON)",
            desc="The selected Excel tables as (possibly nested) JSON datastructures.",
        )
    ],
    config=Config,
)


class Component(process.Process[Config]):
    def __init__(
        self,
        metadata: meta.Component = METADATA,
        con_man: common.ConnectionManager | None = None,
    ):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    @override
    async def run(self):
        logger.info("%s process running", self.name)
        if await self.update_config_from_port("conf"):
            logger.info("%s updated config from conf port", self.name)

        def default_if_nan(value, default: float | None = 0.0, apply_func=None):
            if value is not None:
                # if apply_func and type(value) is str:
                #    return apply_func(value)
                try:
                    if isinstance(value, float) and np.isnan(value):
                        return default
                    elif apply_func:
                        return apply_func(value)
                    else:
                        return value
                except Exception:
                    logger.exception("%s returning default for value %s", self.name, value)
                    return default
            return value

        file = self.config.file
        # file = "/home/berg/GitHub/amei_exercises/ames_bare_soil/AMEI_fallow_Ames_2024-05-16.xlsx"

        data: dict[str, Any] = {}

        # read additional (custom/optional) sheets dynamically and attach them based on their key columns
        def convert_cell(raw, kind: Literal["date", "int", "float", "str"]):
            try:
                if raw is None or (np.isscalar(raw) and pandas.isna(raw)):
                    return None
            except (TypeError, ValueError):
                pass
            try:
                if kind == "date":
                    return str(raw)[:10]
                if kind == "int":
                    return int(raw)
                if kind == "float":
                    return float(raw)
                return str(raw)
            except Exception:
                logger.exception("%s: could not convert value %s as %s", self.name, raw, kind)
                return None

        def add_dynamic_sheet(sheet_name: str, sheet_df: pandas.DataFrame):
            cols = list(sheet_df.columns)
            # the attachment level is determined by the key columns present in the sheet
            # find the deepest nesting level
            no_of_key_cols = 0
            key_cols = []
            inv_k_cols = {}
            for level_name, k_cols in self.config.nesting_by_key_cols.items():
                inv_k_cols["|".join(k_cols)] = level_name
                if all([key_col in cols for key_col in k_cols]):
                    key_cols = k_cols
                    if (no := len(key_cols)) > no_of_key_cols:
                        no_of_key_cols = no

            sub_key = sheet_name.lower()
            # if the key columns are unique per row store a single object, otherwise a list of rows
            as_list = bool(sheet_df.duplicated(subset=key_cols).any()) if len(key_cols) > 0 else True  # False
            # derive a converter per column from its pandas dtype
            kinds: dict[str, Literal["date", "int", "float", "str"]] = {}
            for c in cols:
                dt = str(sheet_df[c].dtype)
                if dt.startswith("datetime"):
                    kinds[c] = "date"
                elif dt.startswith("int"):
                    kinds[c] = "int"
                elif dt.startswith("float"):
                    kinds[c] = "float"
                else:
                    kinds[c] = "str"

            for i in sheet_df.axes[0]:
                target = data
                ref = ""
                cols_so_far = []
                for key_col in key_cols:
                    # create intermediate named levels
                    cols_so_far.append(key_col)
                    cols_so_far_str = "|".join(cols_so_far)
                    if cols_so_far_str in inv_k_cols:
                        level_name = inv_k_cols[cols_so_far_str]
                        if level_name not in target:
                            target[level_name] = {}
                        target = target[level_name]
                        ref += f"{level_name}/"
                    # create the actual data level
                    key_value = str(sheet_df[key_col][i])
                    if key_value not in target:
                        target[key_value] = {}
                    target = target[key_value]
                    ref += f"{key_value}/"

                record = {
                    c: (str(sheet_df[c][i]) if c in key_cols else convert_cell(sheet_df[c][i], kinds[c])) for c in cols
                }
                if as_list:
                    existing = target.get(sub_key)
                    if not isinstance(existing, list):
                        existing = []
                        target[sub_key] = existing
                    existing.append(record)
                else:
                    target[sub_key] = record

        all_sheet_names = pandas.ExcelFile(file).sheet_names
        sheet_names: list[str] = []
        if self.config.load_all:
            sheet_names = all_sheet_names
        else:
            for s in self.config.sheets_to_load:
                if s in all_sheet_names:
                    sheet_names.append(s)
        if len(sheet_names) > 0:
            dynamic_dfs = pandas.read_excel(file, sheet_name=sheet_names, header=self.config.index_of_header_row)
            for s in sheet_names:
                add_dynamic_sheet(s, dynamic_dfs[s])

        if self.out_ports["out"]:
            try:

                async def send_it(d: dict[str, Any], nesting_levels=[]):
                    if len(nesting_levels) > 0:
                        if self.config.include_branch_level_data:
                            await send_it({k: v for k, v in d.items() if k != nesting_levels[0]})
                            if nesting_levels[0] in d:
                                if self.config.include_brackets_when_sending_nested:
                                    if not await self.write_out("out", fbp_capnp.IP.new_message(type="openBracket")):
                                        logger.info("%s: process finished", self.name)
                                        return
                                await send_it(d[nesting_levels[0]], nesting_levels[1:])
                                if self.config.include_brackets_when_sending_nested:
                                    if not await self.write_out("out", fbp_capnp.IP.new_message(type="closeBracket")):
                                        logger.info("%s: process finished", self.name)
                                        return
                    else:
                        out_ip = fbp_capnp.IP.new_message(content=json.dumps(d))
                        # common.copy_and_set_fbp_attrs(in_ip, out_ip, **{self.config.to_attr: attr})
                        if not await self.write_out("out", out_ip):
                            logger.info("%s: process finished", self.name)
                            return

                await send_it(data, self.config.send_nesting_level)

            except capnp.KjException as e:
                logger.exception("%s: RPC Exception: %s", self.name, e.description())

        logger.info("%s: process finished", self.name)


def main():
    process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)


if __name__ == "__main__":
    main()
