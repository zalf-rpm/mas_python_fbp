from __future__ import annotations

from tests.component_harness import done_message, ip_message, run_process_component, text_outputs
from zalfmas_fbp.components.ip.copy import Copy
from zalfmas_fbp.components.ip.copy import meta as copy_meta


def test_copy_broadcasts_copied_ip_to_array_outputs() -> None:
    component = Copy(copy_meta)

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("alpha"),
                done_message(),
            ],
        },
        outputs=(),
        array_outputs={"out": 2},
    )

    first, second = result.array_output("out")
    assert text_outputs(first) == ["alpha"]
    assert text_outputs(second) == ["alpha"]
