"""read_csv, converted from Runnable to Process style (plan LP2)."""

from __future__ import annotations

import json
from pathlib import Path

from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component
from zalfmas_fbp.components.common.values import python_from_attr
from zalfmas_fbp.components.file.read_csv import METADATA, ReadCsv

# fbp.capnp:Component.Port is a convenient target: text, an enum, and a bool field.
PORT_STRUCT = "@0xc28d2829add1cd72 = fbp/fbp.capnp:Component.Port"


def write_csv(tmp_path: Path, text: str, name: str = "rows.csv") -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


def conf(**settings):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings)))


def run(*configs):
    """One config per file to read; the component asks for the next itself."""
    return run_process_component(
        ReadCsv(METADATA),
        inputs={"conf": [*[conf(**c) for c in configs], done_message()]},
        outputs=("out",),
    ).output()


def ports(writer):
    from mas.schema.fbp import fbp_capnp as f

    return [v.content.as_struct(f.Component.Port) for v in writer.values if str(v.type) == "standard"]


def shapes(writer):
    return [str(v.type) for v in writer.values]


CSV = "name,required\nin,true\nout,false\n"


def test_it_is_a_process_component_now() -> None:
    assert METADATA.type == "process"


def test_each_row_becomes_a_typed_struct(tmp_path: Path) -> None:
    writer = run({"file": write_csv(tmp_path, CSV), "struct_type": PORT_STRUCT})
    built = ports(writer)
    assert [(p.name, p.required) for p in built] == [("in", True), ("out", False)]


def test_booleans_written_as_words_are_coerced(tmp_path: Path) -> None:
    """CSV carries booleans as words; the shared converter now handles that."""
    csv_text = "name,required\na,TRUE\nb,no\nc,\n"
    built = ports(run({"file": write_csv(tmp_path, csv_text), "struct_type": PORT_STRUCT}))
    assert [p.required for p in built] == [True, False, False]


def test_the_output_is_tagged_with_the_struct_type(tmp_path: Path) -> None:
    writer = run({"file": write_csv(tmp_path, CSV), "struct_type": PORT_STRUCT})
    assert writer.values[0].sysAttributes.contentType == PORT_STRUCT


def test_columns_can_be_mapped_onto_field_names(tmp_path: Path) -> None:
    csv_text = "port_name,required\nin,true\n"
    built = ports(
        run(
            {
                "file": write_csv(tmp_path, csv_text),
                "struct_type": PORT_STRUCT,
                "col_to_field_names": {"port_name": "name"},
            },
        ),
    )
    assert built[0].name == "in"


def test_unknown_columns_are_ignored_by_default(tmp_path: Path) -> None:
    csv_text = "name,nonsense\nin,x\n"
    built = ports(run({"file": write_csv(tmp_path, csv_text), "struct_type": PORT_STRUCT}))
    assert built[0].name == "in"


def test_send_ids_selects_rows(tmp_path: Path) -> None:
    csv_text = "name,required\na,true\nb,true\nc,true\n"
    built = ports(
        run(
            {
                "file": write_csv(tmp_path, csv_text),
                "struct_type": PORT_STRUCT,
                "id_col": "name",
                "send_ids": ["a", "c"],
            },
        ),
    )
    assert [p.name for p in built] == ["a", "c"]


def test_the_delimiter_can_be_given_instead_of_sniffed(tmp_path: Path) -> None:
    built = ports(
        run({"file": write_csv(tmp_path, "name;required\nin;true\n"), "struct_type": PORT_STRUCT, "delimiter": ";"}),
    )
    assert built[0].name == "in"


def test_the_delimiter_is_sniffed_when_not_given(tmp_path: Path) -> None:
    built = ports(run({"file": write_csv(tmp_path, "name;required\nin;true\n"), "struct_type": PORT_STRUCT}))
    assert built[0].name == "in"


def test_a_row_can_go_into_an_attribute_instead(tmp_path: Path) -> None:
    writer = run({"file": write_csv(tmp_path, CSV), "struct_type": PORT_STRUCT, "to_attr": "setup"})
    attr = writer.values[0].attributes[0]
    assert attr.key == "setup"
    assert attr._has("valueType"), "the struct's type has to travel with it, or it cannot be read"


def test_rows_can_be_wrapped_in_a_substream(tmp_path: Path) -> None:
    writer = run({"file": write_csv(tmp_path, CSV), "struct_type": PORT_STRUCT, "wrap_in_substream": True})
    assert shapes(writer) == ["openBracket", "standard", "standard", "closeBracket"]
    assert python_from_attr(writer.values[-1].attributes[0]) == 2


def test_successive_configs_read_successive_files(tmp_path: Path) -> None:
    """What makes this a source rather than a one-shot: a flow can drive it over several files."""
    first = write_csv(tmp_path, "name,required\na,true\n", "a.csv")
    second = write_csv(tmp_path, "name,required\nb,true\n", "b.csv")

    built = ports(
        run({"file": first, "struct_type": PORT_STRUCT}, {"file": second, "struct_type": PORT_STRUCT}),
    )
    assert [p.name for p in built] == ["a", "b"]


def test_a_missing_file_is_reported_rather_than_crashing(tmp_path: Path) -> None:
    assert run({"file": str(tmp_path / "nope.csv"), "struct_type": PORT_STRUCT}).values == []


def test_an_unresolvable_struct_type_emits_nothing(tmp_path: Path) -> None:
    assert run({"file": write_csv(tmp_path, CSV), "struct_type": "not a type"}).values == []


def test_an_empty_file_emits_nothing(tmp_path: Path) -> None:
    assert run({"file": write_csv(tmp_path, ""), "struct_type": PORT_STRUCT}).values == []
