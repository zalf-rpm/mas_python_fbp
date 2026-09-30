"""create_monica_capnp_env: narrowed error handling (plan LP5).

The per-IP body used to sit inside `try: ... except Exception: logger.exception(...)`, so a
component that failed on every IP was indistinguishable from one with no input. Only the step
that can legitimately fail on bad input - reading and parsing the payload - is guarded now.
"""

from __future__ import annotations

import json

import pytest
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from mas.schema.model import model_capnp

from tests.component_harness import (
    PortMessage,
    PortValue,
    cap_message,
    close_bracket_message,
    done_message,
    ip_message,
    open_bracket_message,
    run_process_component,
)
from tests.fake_services import FakeSoilProfile, FakeTimeSeries
from zalfmas_fbp.components.models.monica.create_monica_capnp_env import METADATA, Component

ENV = {"type": "Env", "params": {"siteParameters": {"Latitude": 52.0}}}


def env_ip(env=None, *, to_attr=None, **attrs):
    text = json.dumps(ENV if env is None else env)
    ip = fbp_capnp.IP.new_message()
    named = dict(attrs)
    if to_attr:
        named[to_attr] = text
    else:
        ip.content = text
    if named:
        entries = ip.init("attributes", len(named))
        for i, (key, value) in enumerate(named.items()):
            entries[i].key = key
            entries[i].value = value
    return PortMessage(PortValue(ip))


def run(messages, *, timeseries=None, soil=None, **settings):
    inputs: dict = {"in": [*messages, done_message()]}
    if timeseries is not None:
        inputs["timeseries"] = timeseries
    if soil is not None:
        inputs["soil"] = soil
    if settings:
        inputs["conf"] = [
            ip_message(common_capnp.StructuredText.new_message(type="json", value=json.dumps(settings))),
            done_message(),
        ]
    return run_process_component(Component(METADATA), inputs=inputs, outputs=("out",))


def standard(writer):
    return [v for v in writer.values if str(v.type) == "standard"]


def env_of(result, index=0):
    return standard(result.output("out"))[index].content.as_struct(model_capnp.Env)


def env_json(result, index=0) -> dict:
    return json.loads(env_of(result, index).rest.as_struct(common_capnp.StructuredText).value)


def test_it_emits_an_env_carrying_the_json() -> None:
    result = run([env_ip()])
    assert env_json(result)["type"] == "Env"


def test_brackets_are_forwarded() -> None:
    result = run([open_bracket_message(), env_ip(), close_bracket_message()])
    assert [str(v.type) for v in result.output("out").values] == [
        "openBracket",
        "standard",
        "closeBracket",
    ]


def test_a_payload_that_is_not_json_is_skipped_by_default() -> None:
    result = run([ip_message("not json at all"), env_ip()])
    assert len(standard(result.output("out"))) == 1


def test_a_json_payload_that_is_not_an_object_is_skipped() -> None:
    result = run([ip_message("[1, 2, 3]"), env_ip()])
    assert len(standard(result.output("out"))) == 1


def test_an_unreadable_payload_can_fail_the_process() -> None:
    with pytest.raises(ValueError, match="no MONICA JSON env"):
        run([ip_message("not json at all")], on_error="fail")


def test_from_attr_reads_the_env_out_of_an_attribute() -> None:
    result = run([env_ip(to_attr="env")], from_attr="env")
    assert env_json(result)["type"] == "Env"


def test_to_attr_puts_the_env_in_an_attribute_instead() -> None:
    result = run([env_ip()], to_attr="env")
    out = standard(result.output("out"))[0]
    assert "env" in [entry.key for entry in out.attributes]


def test_a_timeseries_from_the_port_is_attached() -> None:
    result = run(
        [env_ip()],
        timeseries=[cap_message(lambda: FakeTimeSeries(id_="ts-1")), done_message()],
    )
    assert env_of(result).timeSeries is not None


def test_a_soil_profile_from_the_port_is_attached() -> None:
    result = run([env_ip()], soil=[cap_message(lambda: FakeSoilProfile(id_="p-1")), done_message()])
    assert env_of(result).soilProfile is not None


def test_a_soil_json_sturdy_ref_goes_into_the_site_parameters() -> None:
    layers = [{"Thickness": 0.3}]
    soil_st = common_capnp.StructuredText.new_message(type="json", value=json.dumps(layers))
    result = run([env_ip()], soil=[ip_message(soil_st), done_message()])
    assert env_json(result)["params"]["siteParameters"]["SoilProfileParameters"] == layers


def test_soil_layers_do_not_need_the_env_to_have_the_nesting_already() -> None:
    """The env is arbitrary caller JSON; assuming params.siteParameters exists raised KeyError."""

    layers = [{"Thickness": 0.3}]
    soil_st = common_capnp.StructuredText.new_message(type="json", value=json.dumps(layers))
    result = run([env_ip({"type": "Env"})], soil=[ip_message(soil_st), done_message()])
    assert env_json(result)["params"]["siteParameters"]["SoilProfileParameters"] == layers


def test_a_closed_timeseries_port_does_not_kill_the_component() -> None:
    """This branch assigned to `self.in_port["climate"]` - no such attribute, and the wrong port
    name - which raised AttributeError that the blanket except used to hide."""

    result = run([env_ip(), env_ip()], timeseries=[done_message()])
    assert len(standard(result.output("out"))) == 2


def test_a_closed_soil_port_does_not_kill_the_component() -> None:
    result = run([env_ip(), env_ip()], soil=[done_message()])
    assert len(standard(result.output("out"))) == 2


def test_input_attributes_are_carried_over() -> None:
    result = run([env_ip(region="north")])
    assert "region" in [entry.key for entry in standard(result.output("out"))[0].attributes]


def test_several_ips_in_a_row() -> None:
    result = run([env_ip(), env_ip(), env_ip()])
    assert len(standard(result.output("out"))) == 3


def test_the_component_is_not_silent_about_a_fault_in_itself(monkeypatch) -> None:
    """The point of narrowing: a bug in the component now stops it, rather than logging one
    traceback per IP while the flow quietly produces nothing."""

    def boom(self, in_ip, in_attrs):
        msg = "a fault inside the component"
        raise RuntimeError(msg)

    monkeypatch.setattr(Component, "json_env_of", boom)
    with pytest.raises(RuntimeError, match="a fault inside the component"):
        run([env_ip()])


def test_soil_and_timeseries_capabilities_survive_downstream() -> None:
    """What is attached has to still be callable by whoever receives the Env."""

    async def call_them(result):
        # Env declares these as typed capabilities, so they arrive ready to call
        env = env_of(result)
        return (await env.timeSeries.info()).id, (await env.soilProfile.info()).id

    inputs: dict = {
        "in": [env_ip(), done_message()],
        "timeseries": [cap_message(lambda: FakeTimeSeries(id_="ts-9")), done_message()],
        "soil": [cap_message(lambda: FakeSoilProfile(id_="p-9")), done_message()],
    }
    result = run_process_component(Component(METADATA), inputs=inputs, outputs=("out",), after=call_them)
    assert result.after_result == ("ts-9", "p-9")
