"""Connecting ports is fatal when it fails (plan LP5).

A component that started with some of its ports silently unconnected read nothing and wrote
nothing, which looks exactly like a component with no input - the hardest kind of flow problem to
track down. The three connect paths now raise instead of logging and carrying on.
"""

from __future__ import annotations

import asyncio

import pytest
from capnp.lib.capnp import KjException

from zalfmas_fbp.run.ports import PortConnectionError, PortConnector


class ExplodingConnectionManager:
    """A connection manager whose every connection attempt fails the way an unreachable peer does."""

    def __init__(self, error: BaseException | None = None):
        self.error = error or KjException("could not reach the channel")
        self.attempts = 0

    async def try_connect(self, _sr, retry_secs=1):
        self.attempts += 1
        raise self.error


class NullConnectionManager:
    """A connection manager that answers 'no such capability' rather than failing."""

    def __init__(self):
        self.attempts = 0

    async def try_connect(self, _sr, retry_secs=1):
        self.attempts += 1
        return None


def connector(con_man):
    pc = PortConnector(ins=["in"], outs=["out"])
    pc.con_man = con_man  # pyright: ignore[reportAttributeAccessIssue]
    return pc


def test_a_failing_connection_from_the_cmd_config_is_fatal() -> None:
    con_man = ExplodingConnectionManager()
    pc = connector(con_man)

    with pytest.raises(PortConnectionError, match="command-line config"):
        asyncio.run(pc.connect_from_cmd_config({"in_sr": "capnp://host/in"}))
    assert con_man.attempts == 1


def test_a_failing_connection_from_the_toml_config_is_fatal() -> None:
    pc = connector(ExplodingConnectionManager())

    with pytest.raises(PortConnectionError, match="TOML config"):
        asyncio.run(pc.connect_from_toml_str('[ports.in]\nsr = "capnp://host/in"\n'))


def test_the_original_failure_is_kept_as_the_cause() -> None:
    """So the traceback still says what actually went wrong underneath."""

    original = KjException("the channel was not there")
    pc = connector(ExplodingConnectionManager(original))

    with pytest.raises(PortConnectionError) as caught:
        asyncio.run(pc.connect_from_cmd_config({"in_sr": "capnp://host/in"}))
    assert caught.value.__cause__ is original


def test_a_malformed_toml_config_is_fatal_too() -> None:
    """A config whose shape is wrong is a flow problem, not something to carry on from. This one
    fails in the TOML parser before the connecting starts, so it surfaces as a decode error."""

    import tomllib

    pc = connector(NullConnectionManager())

    with pytest.raises(tomllib.TOMLDecodeError):
        asyncio.run(pc.connect_from_toml_str("this is not toml at all ]["))


def test_an_unconnectable_ref_still_leaves_the_port_unset_rather_than_raising() -> None:
    """`try_connect` answering None is not a failure - it is how an intentionally unconnected
    port is expressed - so it must not be turned into an error."""

    con_man = NullConnectionManager()
    pc = connector(con_man)

    asyncio.run(pc.connect_from_cmd_config({"in_sr": "capnp://host/in"}))
    assert pc.in_ports["in"] is None
    assert con_man.attempts == 1


def test_an_explicitly_null_port_connects_nothing_and_does_not_raise() -> None:
    con_man = NullConnectionManager()
    pc = connector(con_man)

    asyncio.run(pc.connect_from_cmd_config({"in_sr": None, "out_sr": None}))
    assert pc.in_ports["in"] is None
    assert pc.out_ports["out"] is None
    assert con_man.attempts == 0
