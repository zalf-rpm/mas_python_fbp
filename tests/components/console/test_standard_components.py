from __future__ import annotations

from typing import Any

from tests.component_harness import done_message, ip_message, run_standard_component
from zalfmas_fbp.components.console.console_output import run_component


def test_console_output_writes_text_content_to_stdout(capsys: Any, monkeypatch: Any) -> None:
    run_standard_component(
        run_component,
        monkeypatch,
        inputs={
            "in": [
                ip_message("hello console"),
                done_message(),
            ],
        },
    )

    captured = capsys.readouterr()
    assert captured.out == "hello console\n"
