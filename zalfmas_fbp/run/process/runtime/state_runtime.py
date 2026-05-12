from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, override

import capnp
from mas.schema.fbp import fbp_capnp

from zalfmas_fbp.run.process.context import ProcessStatusState
from zalfmas_fbp.run.process.identity import ProcessIdentityContext

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import ActivityInfoBuilder
    from mas.schema.fbp.fbp_capnp.types.clients import (
        ActivityTransitionClient,
        StateTransitionClient,
    )
    from mas.schema.fbp.fbp_capnp.types.enums import (
        ProcessActivityStateEnum,
        ProcessStateEnum,
    )
    from mas.schema.fbp.fbp_capnp.types.readers import ActivityInfoReader

logger = logging.getLogger(__name__)


class StateTransition(fbp_capnp.Process.StateTransition.Server):
    def __init__(
        self,
        callback: Callable[[ProcessStateEnum, ProcessStateEnum], None],
    ):
        self.callback: Callable[[ProcessStateEnum, ProcessStateEnum], None] = callback

    @override
    async def stateChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class ActivityTransition(fbp_capnp.Process.ActivityTransition.Server):
    def __init__(
        self,
        callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ],
    ):
        self.callback: Callable[
            [ActivityInfoBuilder | ActivityInfoReader, ActivityInfoBuilder | ActivityInfoReader],
            None,
        ] = callback

    @override
    async def activityChanged(self, old, new, _context, **kwargs):
        self.callback(old, new)


class ProcessActivityContext(Protocol):
    async def transition_to_activity(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
        delay_processing: bool = True,
    ) -> None: ...


class ProcessStateRuntime:
    def __init__(
        self,
        *,
        identity: ProcessIdentityContext,
        state: ProcessStatusState,
        processing_delay_milliseconds: int,
    ) -> None:
        self._identity: ProcessIdentityContext = identity
        self._state: ProcessStatusState = state
        self._processing_delay_milliseconds: int = processing_delay_milliseconds

    def activity_message(self) -> ActivityInfoBuilder:
        return fbp_capnp.Process.ActivityInfo.new_message(
            state=self._state.activity_state,
            port=self._state.activity_port,
        )

    async def transition_to_state(self, new_state: ProcessStateEnum) -> None:
        if new_state == self._state.process_state:
            return

        previous_state = self._state.process_state
        self._state.process_state = new_state
        await self._notify_state_transition_callbacks(previous_state, new_state)

    async def _notify_state_transition_callbacks(
        self,
        old_state: ProcessStateEnum,
        new_state: ProcessStateEnum,
    ) -> None:
        active_callbacks: list[StateTransitionClient] = []
        for callback in self._state.state_transition_callbacks:
            try:
                await callback.stateChanged(old_state, new_state)
            except (capnp.KjException, RuntimeError) as error:
                logger.warning(
                    "%s state transition callback failed and will be removed: %s",
                    self._identity.name,
                    error,
                )
            else:
                active_callbacks.append(callback)
        self._state.state_transition_callbacks = active_callbacks

    async def _notify_activity_transition_callbacks(
        self,
        old_info: ActivityInfoBuilder,
        new_info: ActivityInfoBuilder,
    ) -> None:
        active_callbacks: list[ActivityTransitionClient] = []
        for callback in self._state.activity_transition_callbacks:
            try:
                await callback.activityChanged(old_info, new_info)
            except (capnp.KjException, RuntimeError) as error:
                logger.warning(
                    "%s activity transition callback failed and will be removed: %s",
                    self._identity.name,
                    error,
                )
            else:
                active_callbacks.append(callback)
        self._state.activity_transition_callbacks = active_callbacks

    def cancel_pending_processing_activity(self) -> None:
        task = self._state.pending_processing_activity_task
        self._state.pending_processing_activity_task = None
        if task is not None and not task.done():
            _ = task.cancel()

    async def _transition_to_activity_now(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
    ) -> None:
        new_port = port or ""
        if new_state == self._state.activity_state and new_port == self._state.activity_port:
            return

        old_info = self.activity_message()
        self._state.activity_state = new_state
        self._state.activity_port = new_port
        new_info = self.activity_message()
        await self._notify_activity_transition_callbacks(old_info, new_info)

    async def _commit_processing_activity_after_delay(self, delay_milliseconds: int) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(delay_milliseconds / 1000)
            if self._state.pending_processing_activity_task is not task:
                return
            self._state.pending_processing_activity_task = None
            await self._transition_to_activity_now("processing")
        except asyncio.CancelledError:
            pass

    async def transition_to_activity(
        self,
        new_state: ProcessActivityStateEnum,
        port: str | None = None,
        delay_processing: bool = True,
    ) -> None:
        if new_state == "processing":
            if self._state.activity_state == "processing" and self._state.activity_port == "":
                return
            self.cancel_pending_processing_activity()
            if not delay_processing or self._processing_delay_milliseconds <= 0:
                await self._transition_to_activity_now("processing")
                return
            self._state.pending_processing_activity_task = asyncio.create_task(
                self._commit_processing_activity_after_delay(self._processing_delay_milliseconds),
                name=f"{self._identity.name or self._identity.id}-processing-activity-delay",
            )
            return

        self.cancel_pending_processing_activity()
        await self._transition_to_activity_now(new_state, port)
