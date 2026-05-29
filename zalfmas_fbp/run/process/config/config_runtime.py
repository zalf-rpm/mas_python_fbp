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
        next_raw_config = self._state.raw_config.copy()
        for key, value in config_values.items():
            if value is None:
                _ = next_raw_config.pop(key, None)
                continue
            next_raw_config[key] = value

        next_config = self.validate_config(next_raw_config)
        self._state.raw_config = next_raw_config
        self._state.config = next_config
