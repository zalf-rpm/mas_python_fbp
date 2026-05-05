from __future__ import annotations

from mas.schema.common import common_capnp

from tests.component_harness import done_message, ip_message, run_process_component, text_outputs
from zalfmas_fbp.components.ip.copy import Copy
from zalfmas_fbp.components.ip.copy import meta as copy_meta
from zalfmas_fbp.components.ip.load_balancer import LoadBalancer
from zalfmas_fbp.components.ip.load_balancer import meta as load_balancer_meta


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


def test_load_balancer_distributes_ips_round_robin_across_array_outputs() -> None:
    component = LoadBalancer(load_balancer_meta)

    result = run_process_component(
        component,
        inputs={
            "in": [
                ip_message("alpha"),
                ip_message("beta"),
                ip_message("gamma"),
                done_message(),
            ],
        },
        outputs=(),
        array_outputs={"out": 2},
    )

    assert component.config["distribution_strategy"].t == "next_available"
    first, second = result.array_output("out")
    assert text_outputs(first) == ["alpha", "gamma"]
    assert text_outputs(second) == ["beta"]


def test_load_balancer_reads_conf_port_before_processing_input() -> None:
    component = LoadBalancer(load_balancer_meta)

    result = run_process_component(
        component,
        inputs={
            "conf": [
                ip_message(
                    common_capnp.StructuredText.new_message(
                        type="toml",
                        value='distribution_strategy = "round_robin"',
                    ),
                ),
                done_message(),
            ],
            "in": [
                ip_message("alpha"),
                ip_message("beta"),
                ip_message("gamma"),
                done_message(),
            ],
        },
        outputs=(),
        array_outputs={"out": 2},
    )

    assert component.config["distribution_strategy"].t == "round_robin"
    first, second = result.array_output("out")
    assert text_outputs(first) == ["alpha", "gamma"]
    assert text_outputs(second) == ["beta"]
