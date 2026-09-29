from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from zalfmas_fbp.run.metadata import CONFIG_PORT_NAME
from zalfmas_fbp.run.process.config.config_codec import config_from_ip
from zalfmas_fbp.run.process.errors import ProcessConfigError

if TYPE_CHECKING:
    from zalfmas_fbp.run.process.config.config_runtime import ProcessConfigRuntime
    from zalfmas_fbp.run.process.identity import ProcessIdentityContext
    from zalfmas_fbp.run.process.types import ConfigValue

    from .input_runtime import InputRuntime

logger = logging.getLogger(__name__)

#: How long to wait for the initial config before saying out loud that we are still waiting.
INITIAL_CONFIG_WARN_SECONDS = 15.0


class ConfigWatcher:
    """Owns the runtime's ``conf`` port and applies what arrives on it between IPs.

    Components no longer read ``conf`` themselves (plan section 6.1). Two properties matter:

    - The *initial* config is applied before ``run()`` starts, which is what the old
      ``update_config_from_port`` call at the top of every ``run()`` guaranteed by blocking.
    - Later updates are *staged* and applied at an IP boundary - immediately before
      ``read_in`` hands an IP over - never part way through processing one. That makes "config
      changes take effect between IPs" a rule that can be stated and tested, and keeps components
      that snapshot config before their loop correct.
    """

    def __init__(
        self,
        *,
        identity: ProcessIdentityContext,
        input_runtime: InputRuntime,
        config_runtime: ProcessConfigRuntime,
        stop_event: asyncio.Event,
        port_name: str = CONFIG_PORT_NAME,
    ) -> None:
        self._identity: ProcessIdentityContext = identity
        self._input_runtime: InputRuntime = input_runtime
        self._config_runtime: ProcessConfigRuntime = config_runtime
        self._stop_event: asyncio.Event = stop_event
        self._port_name: str = port_name
        self._pending: dict[str, ConfigValue | None] = {}
        self._task: asyncio.Task[None] | None = None
        #: Set whenever an update is staged, so next_config() can wait for one.
        self._staged: asyncio.Event = asyncio.Event()
        #: True once the conf port is done, so a `while await next_config()` loop terminates.
        self._closed: bool = False
        self.applied_updates: int = 0

    @property
    def port_name(self) -> str:
        return self._port_name

    @property
    def connected(self) -> bool:
        return self._input_runtime.in_ports.get(self._port_name) is not None

    async def _read_one(self) -> bool:
        """Read one config IP and stage it. Returns False when the port is done."""
        in_ip = await self._input_runtime.read_in_raw(self._port_name)
        if in_ip is None:
            self._closed = True
            self._staged.set()
            return False
        try:
            self._pending.update(config_from_ip(in_ip))
            self._staged.set()
        except ProcessConfigError:
            logger.exception("%s received invalid config on port %r", self._identity.name, self._port_name)
        return True

    async def prime(self) -> bool:
        """Wait for and apply the first config, so ``run()`` starts already configured.

        Returns False immediately when ``conf`` is unconnected, which is the usual case: the flow
        runner configures Process components over ``setConfigEntry`` instead.
        """
        if not self.connected:
            return False

        read_task = asyncio.ensure_future(self._read_one())
        stop_task = asyncio.ensure_future(self._stop_event.wait())
        try:
            while True:
                done, _pending = await asyncio.wait(
                    {read_task, stop_task},
                    timeout=INITIAL_CONFIG_WARN_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    # A started read is a claim on a message, so it is left to the watch task
                    # rather than cancelled; see InputRuntime for why dropping one loses an IP.
                    return False
                if read_task in done:
                    break
                logger.warning(
                    "%s is still waiting for its initial config on port %r.",
                    self._identity.name,
                    self._port_name,
                )
        finally:
            _ = stop_task.cancel()

        if not read_task.result():
            return False
        return self.apply_pending()

    def start(self) -> None:
        """Watch ``conf`` for further updates in the background."""
        if self._task is not None or not self.connected:
            return
        self._task = asyncio.create_task(
            self._watch(),
            name=f"{self._identity.name or self._identity.id}-config-watch",
        )

    async def _watch(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not await self._read_one():
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s config watch failed; stopping it", self._identity.name)
                return

    async def next_config(self) -> bool:
        """Wait until a config update has been applied. Returns False once no more can come.

        Lets a component with no data in-port drive itself from its ``conf`` port - a file reader
        fed a new path per config, say::

            while True:
                emit_file(self.config.file)
                if not await self.next_config():
                    break

        Returns False immediately when ``conf`` is unconnected, and once the port closes, so such a
        loop always terminates.
        """
        if self.apply_pending():
            return True
        if self._closed or not self.connected:
            return False

        self._staged.clear()
        staged_task = asyncio.ensure_future(self._staged.wait())
        stop_task = asyncio.ensure_future(self._stop_event.wait())
        try:
            _done, _pending = await asyncio.wait(
                {staged_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (staged_task, stop_task):
                if not task.done():
                    _ = task.cancel()

        if self._stop_event.is_set():
            return False
        return self.apply_pending()

    def apply_pending(self) -> bool:
        """Apply staged config. Called at IO boundaries, so never mid-processing."""
        if not self._pending:
            return False
        staged, self._pending = self._pending, {}
        try:
            self._config_runtime.apply_config_values(staged)
        except Exception:
            logger.exception("%s could not apply config %s; keeping the previous one", self._identity.name, staged)
            return False
        self.applied_updates += 1
        logger.info("%s applied config update: %s", self._identity.name, sorted(staged))
        return True

    async def close(self) -> None:
        task = self._task
        self._task = None
        if task is None or task.done():
            return
        _ = task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            logger.debug("%s config watch task ended", self._identity.name, exc_info=True)
