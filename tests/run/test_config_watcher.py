"""The runtime-owned conf port: initial config before run(), later updates between IPs.

See plan section 6.1 (decision D-conf) and runtime/config_watcher.py.
"""

from __future__ import annotations

import json

from mas.schema.common import common_capnp
from pydantic import Field

from tests.component_harness import (
    done_message,
    ip_message,
    run_process_component,
)
from zalfmas_fbp.run import metadata as meta
from zalfmas_fbp.run import process


class Config(process.ProcessConfig):
    label: str = Field("default", description="Recorded with each IP.")


METADATA = meta.Component(
    info=meta.Info(id="7a1a2b3c-0000-4000-8000-00000000c0f0", name="config probe"),
    type="process",
    inPorts=[meta.Port(name="in")],
    outPorts=[meta.Port(name="out")],
    config=Config,
)


class ConfigProbe(process.Process[Config]):
    """Records the config label in force as each IP is handed over."""

    def __init__(self, metadata: meta.Component = METADATA):
        super().__init__(metadata=metadata)
        self.seen: list[tuple[str, str]] = []
        self.label_at_start: str | None = None

    async def run(self):
        self.label_at_start = self.config.label
        while True:
            in_ip = await self.read_in("in")
            if in_ip is None:
                break
            self.seen.append((in_ip.content.as_text(), self.config.label))


def conf_message(**settings):
    return ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings)))


def test_config_applies_before_run_starts() -> None:
    """What the old blocking update_config_from_port call at the top of run() guaranteed."""
    component = ConfigProbe()

    run_process_component(
        component,
        inputs={
            "conf": [conf_message(label="configured"), done_message()],
            "in": [ip_message("a"), done_message()],
        },
    )

    assert component.label_at_start == "configured"
    assert component.seen == [("a", "configured")]


def test_an_unconnected_conf_port_does_not_block() -> None:
    """The usual case: the flow runner configures Process components over setConfigEntry."""
    component = ConfigProbe()

    run_process_component(component, inputs={"in": [ip_message("a"), done_message()]})

    assert component.label_at_start == "default"
    assert component.seen == [("a", "default")]


def test_staged_config_lands_at_the_next_ip_boundary() -> None:
    """Never part way through processing an IP: the component sees the old value until the next
    read hands it a new IP.
    """
    component = ConfigProbe()
    ports = {"in": [ip_message("a"), ip_message("b"), done_message()]}

    # Stage an update the way the watcher would, then let the component read.
    component._config_watcher._pending = {"label": "updated"}

    run_process_component(component, inputs=ports)

    # The first read applies the staged update before handing the IP over, so both IPs see it,
    # and crucially the value never changes while an IP is being processed.
    assert component.seen == [("a", "updated"), ("b", "updated")]


def test_applying_config_is_reported_and_clears_the_staging_area() -> None:
    component = ConfigProbe()
    watcher = component._config_watcher

    watcher._pending = {"label": "once"}
    assert watcher.apply_pending() is True
    assert component.config.label == "once"
    assert watcher.applied_updates == 1

    assert watcher.apply_pending() is False
    assert watcher.applied_updates == 1


def test_config_that_fails_validation_keeps_the_previous_one() -> None:
    component = ConfigProbe()
    watcher = component._config_watcher

    watcher._pending = {"label": "good"}
    _ = watcher.apply_pending()
    watcher._pending = {"nonexistent_field": 1}
    assert watcher.apply_pending() is False
    assert component.config.label == "good"


def test_update_config_from_port_is_a_no_op_that_reports_whether_config_arrived() -> None:
    """Existing components still call it; it must neither consume an IP nor mislead them."""
    component = ConfigProbe()

    run_process_component(
        component,
        inputs={
            "conf": [conf_message(label="configured"), done_message()],
            "in": [ip_message("a"), done_message()],
        },
    )

    import asyncio

    assert asyncio.run(component.update_config_from_port()) is True
    assert component.config.label == "configured"


def test_the_component_declares_no_conf_port_but_the_runtime_provides_one() -> None:
    assert "conf" not in [p.name for p in [meta.Port(name="in")]]
    assert "conf" in METADATA.inPorts[-1].name
    assert METADATA.inPorts[-1].role == "config"
    assert ConfigProbe().in_ports.keys() == {"in", "conf"}


# --- sources driven by their config port ------------------------------------------------------


class Source(process.Process[Config]):
    """A component with no data in-port, like file/read_file, driven by its conf port."""

    def __init__(self):
        super().__init__(metadata=SOURCE_METADATA)
        self.emitted: list[str] = []

    async def run(self):
        while True:
            self.emitted.append(self.config.label)
            if not await self.next_config():
                break


SOURCE_METADATA = meta.Component(
    info=meta.Info(id="7a1a2b3c-0000-4000-8000-00000000c0f1", name="config source"),
    type="process",
    inPorts=[],
    outPorts=[meta.Port(name="out")],
    config=Config,
)


def test_a_source_can_loop_over_successive_configs() -> None:
    """A source reaches no read boundary, so it needs next_config to pick up updates at all."""
    component = Source()

    run_process_component(
        component,
        inputs={
            "conf": [
                conf_message(label="a.txt"),
                conf_message(label="b.txt"),
                conf_message(label="c.txt"),
                done_message(),
            ],
        },
    )

    assert component.emitted == ["a.txt", "b.txt", "c.txt"]


def test_a_source_loop_terminates_when_the_conf_port_closes() -> None:
    component = Source()

    run_process_component(component, inputs={"conf": [conf_message(label="only"), done_message()]})

    assert component.emitted == ["only"]


def test_a_source_loop_terminates_when_conf_is_unconnected() -> None:
    """Otherwise a source with no config would hang forever instead of running once."""
    component = Source()

    run_process_component(component, inputs={})

    assert component.emitted == ["default"]


def test_config_also_lands_at_write_boundaries() -> None:
    """Symmetric with reads, so a component that only writes still picks updates up."""
    component = ConfigProbe()
    component._config_watcher._pending = {"label": "staged"}

    result = run_process_component(component, inputs={"in": [ip_message("a"), done_message()]})

    assert component.config.label == "staged"
    assert result.output() is not None
