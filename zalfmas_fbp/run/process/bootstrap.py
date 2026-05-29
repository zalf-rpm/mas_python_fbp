from __future__ import annotations

from zalfmas_fbp.run.metadata import ComponentMetadata, ComponentPortMetadata

from .config.config_runtime import ProcessConfigRuntime
from .context import ProcessPortState
from .identity import WritableProcessIdentityContext
from .types import ConfigValue, ProcessConfig, RawConfig


class ProcessBootstrap[ConfigT: ProcessConfig | RawConfig]:
    def __init__(
        self,
        *,
        metadata: ComponentMetadata | None,
        ports: ProcessPortState,
        config_runtime: ProcessConfigRuntime[ConfigT],
        identity: WritableProcessIdentityContext,
    ) -> None:
        self._metadata: ComponentMetadata | None = metadata
        self._ports: ProcessPortState = ports
        self._config_runtime: ProcessConfigRuntime[ConfigT] = config_runtime
        self._identity: WritableProcessIdentityContext = identity

    @staticmethod
    def _is_array_port(port_info: ComponentPortMetadata) -> bool:
        return port_info.type == "array"

    def initialize_from_metadata(self) -> None:
        default_config: dict[str, ConfigValue] = {}
        component_meta = self._metadata
        if component_meta is not None:
            default_config = component_meta.default_config_values()
            self._identity.name = component_meta.info.name
            if component_meta.info.description is not None:
                self._identity.description = component_meta.info.description
            for port_info in component_meta.inPorts:
                name = port_info.name
                if self._is_array_port(port_info):
                    _ = self._ports.array_in_ports.setdefault(name, [])
                    _ = self._ports.array_in_buffers.setdefault(name, {})
                else:
                    _ = self._ports.in_ports.setdefault(name, None)
            for port_info in component_meta.outPorts:
                name = port_info.name
                if self._is_array_port(port_info):
                    _ = self._ports.array_out_ports.setdefault(name, [])
                    _ = self._ports.array_out_next_indices.setdefault(name, 0)
                    _ = self._ports.array_out_write_tasks.setdefault(name, [])
                else:
                    _ = self._ports.out_ports.setdefault(name, None)

        filtered_defaults = {key: value for key, value in default_config.items() if value is not None}
        self._config_runtime.apply_config_values(filtered_defaults)
