from __future__ import annotations

from typing import Any, cast

import zalfmas_fbp.run.ports as ports
from tests.component_harness import InMemoryReader, done_message, ip_message, run_standard_component
from zalfmas_fbp.components.console.console_output import run_component


def test_console_output_writes_text_content_to_stdout(capsys: Any, monkeypatch: Any) -> None:
    port_connector = ports.PortConnector(ins=["in"])
    port_connector.in_ports["in"] = cast(
        Any,
        InMemoryReader(
            [
                ip_message("hello console"),
                done_message(),
            ]
        ),
    )

    run_standard_component(run_component, port_connector, monkeypatch)

    captured = capsys.readouterr()
    assert captured.out == "hello console\n"
