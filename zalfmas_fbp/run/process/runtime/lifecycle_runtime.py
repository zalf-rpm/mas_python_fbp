from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from zalfmas_fbp.run.logging_config import format_exception_full
from zalfmas_fbp.run.process.context import ProcessLifecycleState, ProcessStatusState
from zalfmas_fbp.run.process.errors import ProcessErrorInfo
from zalfmas_fbp.run.process.identity import ProcessIdentityContext

from .input_runtime import InputRuntime
from .output_runtime import OutputRuntime
from .state_runtime import ProcessStateRuntime

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.enums import ProcessStateEnum

logger = logging.getLogger(__name__)


class ProcessLifecycleRuntime:
    def __init__(
        self,
        *,
        identity: ProcessIdentityContext,
        lifecycle: ProcessLifecycleState,
        status: ProcessStatusState,
        state_runtime: ProcessStateRuntime,
        input_runtime: InputRuntime,
        output_runtime: OutputRuntime,
        run_fn: Callable[[], Awaitable[None]],
    ) -> None:
        self._identity: ProcessIdentityContext = identity
        self._lifecycle: ProcessLifecycleState = lifecycle
        self._status: ProcessStatusState = status
        self._state_runtime: ProcessStateRuntime = state_runtime
        self._input_runtime: InputRuntime = input_runtime
        self._output_runtime: OutputRuntime = output_runtime
        self._run_fn: Callable[[], Awaitable[None]] = run_fn

    async def start(self) -> bool:
        lifecycle = self._lifecycle
        if lifecycle.run_task is not None and not lifecycle.run_task.done():
            return False
        if self._status.process_state in ("starting", "running", "stopping", "closed"):
            return False

        lifecycle.stop_requested.clear()
        lifecycle.run_exception = None
        lifecycle.last_error = None
        lifecycle.run_task = asyncio.create_task(
            self._run_wrapper(),
            name=f"{self._identity.name or self._identity.id}-run",
        )
        return True

    async def _run_wrapper(self) -> None:
        lifecycle = self._lifecycle
        final_state: ProcessStateEnum = "idle"
        try:
            if lifecycle.stop_requested.is_set():
                return
            await self._state_runtime.transition_to_state("starting")
            if lifecycle.stop_requested.is_set():
                return

            await self._state_runtime.transition_to_state("running")
            await self._state_runtime.transition_to_activity("processing")
            await self._run_fn()
        except asyncio.CancelledError as error:
            if not lifecycle.stop_requested.is_set():
                final_state = "failed"
                self.record_error(error)
                logger.exception("%s run task was cancelled unexpectedly", self._identity.name)
                raise
        except Exception as error:
            final_state = "failed"
            self.record_error(error)
            logger.exception("%s process failed", self._identity.name)
        finally:
            try:
                await self._state_runtime.transition_to_activity("closing")
                await self._output_runtime.close_out_ports()
            except Exception as error:
                if final_state != "failed":
                    final_state = "failed"
                    self.record_error(error)
                logger.exception("%s process failed while closing output ports", self._identity.name)
            if self._status.process_state != "closed":
                await self._state_runtime.transition_to_state(final_state)
            await self._state_runtime.transition_to_activity("none")
            lifecycle.stop_requested.clear()

    def record_error(self, exc: BaseException) -> None:
        lifecycle = self._lifecycle
        lifecycle.run_exception = exc
        cause = exc.__cause__ or exc.__context__
        lifecycle.last_error = ProcessErrorInfo(
            process_id=self._identity.id,
            process_name=self._identity.name,
            phase=cast("str", getattr(exc, "phase", "run")),
            port=cast("str | None", getattr(exc, "port", None)),
            error_type=type(exc).__name__,
            message=str(exc),
            cause_type=type(cause).__name__ if cause is not None else None,
            cause_message=str(cause) if cause is not None else None,
            traceback=format_exception_full(exc),
        )

    async def stop(self) -> bool:
        lifecycle = self._lifecycle
        status = self._status
        has_running_task = lifecycle.run_task is not None and not lifecycle.run_task.done()
        if not has_running_task and status.process_state in ("idle", "failed", "closed"):
            return False

        lifecycle.stop_requested.set()
        await self._state_runtime.transition_to_state("stopping")
        return await self._wait_for_run_task()

    async def _wait_for_run_task(self) -> bool:
        run_task = self._lifecycle.run_task
        if run_task is None:
            return True
        if run_task.done():
            await self._consume_run_task_result()
            return True

        timeout_seconds = self._lifecycle.soft_stop_timeout_seconds
        done, _pending = await asyncio.wait({run_task}, timeout=timeout_seconds)
        if run_task not in done:
            logger.error(
                "%s run task did not stop within %.3fs",
                self._identity.name,
                timeout_seconds,
            )
            return False

        await self._consume_run_task_result()
        return True

    async def _consume_run_task_result(self) -> None:
        run_task = self._lifecycle.run_task
        if run_task is None:
            return
        with contextlib.suppress(asyncio.CancelledError):
            await run_task

    async def close_in_ports(self) -> None:
        await self._input_runtime.close_in_ports()

    async def force_close_ports(self) -> None:
        await self._state_runtime.transition_to_activity("closing")
        await self.close_in_ports()
        await self.close_out_ports(cancel_pending_writes=True)
        await self._state_runtime.transition_to_activity("none")

    async def close_out_ports(self, *, cancel_pending_writes: bool | None = None) -> None:
        await self._output_runtime.close_out_ports(cancel_pending_writes=cancel_pending_writes)
