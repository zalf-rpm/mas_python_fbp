"""Substream handling in the last components that lacked it (plan LP4).

The two sinks are the interesting ones: a bracket IP was processed like data, so it produced a
spurious output file and advanced the running count used in filename patterns.
"""

from __future__ import annotations

import json
from pathlib import Path

from mas.schema.common import common_capnp

from tests.component_harness import (
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from zalfmas_fbp.components.file.write_file import METADATA as WRITE_META
from zalfmas_fbp.components.file.write_file import WriteFile
from zalfmas_fbp.components.string.to_string import METADATA as TO_STRING_META
from zalfmas_fbp.components.string.to_string import ToString


def conf(**settings):
    return [
        ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
        done_message(),
    ]


def shapes(writer):
    return [str(v.type) for v in writer.values]


# --- to_string ----------------------------------------------------------------------------------


def test_to_string_forwards_brackets_rather_than_stringifying_them() -> None:
    """It emitted a new standard IP for every input, so a bracket became data."""
    writer = run_process_component(
        ToString(TO_STRING_META),
        inputs={
            "in": [
                open_bracket_message(),
                ip_message(common_capnp.StructuredText.new_message(type="json", value='"x"')),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=("out",),
    ).output()

    assert shapes(writer) == ["openBracket", "standard", "closeBracket"]


# --- write_file ---------------------------------------------------------------------------------


def test_write_file_ignores_brackets(tmp_path: Path) -> None:
    component = WriteFile(WRITE_META)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "out_{count}.txt"},
    )

    run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message("a"),
                ip_message("b"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=(),
    )

    written = sorted(p.name for p in tmp_path.iterdir())
    assert written == ["out_0.txt", "out_1.txt"], "a bracket produced a file, or skewed {count}"
    assert (tmp_path / "out_0.txt").read_text() == "a"
    assert (tmp_path / "out_1.txt").read_text() == "b"


def test_write_file_count_matches_data_ips_across_substreams(tmp_path: Path) -> None:
    """{count} should number the data written, not every IP that went past."""
    component = WriteFile(WRITE_META)
    component.apply_config_values(
        {"path_to_out_dir": str(tmp_path), "filename_pattern": "f{count}.txt"},
    )

    run_process_component(
        component,
        inputs={
            "in": [
                open_bracket_message(),
                ip_message("a"),
                close_bracket_message(),
                open_bracket_message(),
                ip_message("b"),
                close_bracket_message(),
                done_message(),
            ],
        },
        outputs=(),
    )

    assert sorted(p.name for p in tmp_path.iterdir()) == ["f0.txt", "f1.txt"]
