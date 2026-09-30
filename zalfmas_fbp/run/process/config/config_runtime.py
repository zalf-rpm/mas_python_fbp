from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from zalfmas_fbp.run.process.context import ProcessConfigState
from zalfmas_fbp.run.process.types import ConfigValue, ProcessConfig, RawConfig


class ProcessConfigRuntime[ConfigT: ProcessConfig | RawConfig]:
    def __init__(
        self,
        *,
        state: ProcessConfigState,
        config_model: type[ProcessConfig] | None,
    ) -> None:
        self._state: ProcessConfigState = state
        self._config_model: type[ProcessConfig] | None = config_model

    def validate_config(self, raw_config: RawConfig) -> ConfigT | RawConfig:
        if self._config_model is None:
            return raw_config
        return cast("ConfigT", self._config_model.model_validate(raw_config))

    def sync_config(self) -> None:
        self._state.config = self.validate_config(self._state.raw_config)

    def apply_config_values(self, config_values: Mapping[str, ConfigValue | None]) -> None:
        """Merge values into the config and revalidate the whole of it.

        ``None`` is an ordinary value, not a request to unset: a field declared ``str | None``
        takes it, and one declared ``str`` is rejected by the model. Removing the key instead
        would silently restore the field's default, which is a different value from the one that
        was asked for - and it would make "set this to null" unexpressible for a caller sending
        incremental changes.

        The merged config is validated as a whole, so setting a single key still checks every
        field, and nothing is committed unless validation passes.
        """
        next_raw_config = self._state.raw_config.copy()
        next_raw_config.update(config_values)

        next_config = self.validate_config(next_raw_config)
        self._state.raw_config = next_raw_config
        self._state.config = next_config
