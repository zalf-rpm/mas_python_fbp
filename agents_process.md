# Process-style component notes for `mas_python_fbp`

This file summarizes practical rules and implementation hints from existing **Process-style** components under `zalfmas_fbp/components/` (for example `string/split_string2.py`, `string/to_string.py`, `models/monica/create_monica_capnp_env.py`, `json/update_json.py`, `ip/*`).

The goal is to make creating new components (or migrating old `standard` components) faster and more consistent.

## 1. Canonical Process-style structure

Use this shape every time:

1. Define a typed config model:
   - `class Config(process.ProcessConfig): ...` with `pydantic.Field(...)`.
2. Define `METADATA = meta.Component(...)`:
   - `type="process"`
   - explicit `inPorts` / `outPorts`
   - `config=Config` (**not** `defaultConfig`).
3. Implement class:
   - `class Component(process.Process[Config]):`
   - `__init__(metadata=METADATA, con_man=None)` calling `super().__init__(...)`.
   - `async def run(self): ...`
4. Provide `main()`:
   - `process.run_process_from_metadata_and_cmd_args(Component(METADATA), METADATA)`.

## 2. Metadata rules that matter

- `info.id` must be UUID4 and unique.
- The component is only discoverable via local service if `configs/local_cmds.json` contains an entry:
  - key = **exactly** `info.id`
  - value = module command, e.g. `python -m zalfmas_fbp.components.json.filter_json`.
- Keep `contentType` and port naming aligned with behavior (`in`, `out`, optional `conf`, plus domain ports).
- For multi-target output, mark out port as array (`type="array"`), e.g. `ip/copy_ip.py`, `ip/load_balancer.py`.

## 3. Typical `run()` loop patterns

### 3.1 Config update pattern

Most Process components do this once at start:

```python
if await self.update_config_from_port("conf"):
    logger.info("%s updated config from conf port", self.name)
```

Use only if `conf` port exists.

### 3.2 Core read/write pattern

```python
while True:
    in_msg = await self.read_in("in")
    if in_msg is None:
        break
    out_ip = fbp_capnp.IP.new_message(content=...)
    if not await self.write_out("out", out_ip):
        return
```

`read_in(...) -> None` means upstream done/disconnected.

### 3.3 Conditional loops by port availability

Existing components often guard with connected ports:

- `while self.in_ports["in"] and self.out_ports["out"]:` (single out)
- `while any(self.array_out_ports["out"]):` (array out only)
- combined guards for multi-input components (`ip/add_attribute.py`).

## 4. Port handling details from existing components

### 4.1 Bracket/substream IPs

If component should preserve stream grouping:
- detect `in_msg.type in ("openBracket", "closeBracket")`
- forward unchanged or explicitly create bracket IPs (`ip/wrap_into_substream.py`).

If not needed, treat input as normal standard IPs only.

### 4.2 Array outputs

Use `write_array_out(...)` with `ArrayOutStrategy`:
- `BROADCAST` (`ip/copy_ip.py`)
- `NEXT_AVAILABLE` / `ROUND_ROBIN` (`ip/load_balancer.py`).

### 4.3 Attribute propagation

Common safe pattern:

```python
common.copy_and_set_fbp_attrs(in_ip, out_ip, **extra_attrs)
```

Some components manually map attrs:

```python
attrs = {kv.key: kv.value for kv in in_ip.attributes}
out_ip.attributes = list([{"key": k, "value": v} for k, v in attrs.items()])
```

Use helper where possible; manual mapping when dynamic mutation is needed.

### 4.4 Content typing / schema detection

For dynamic AnyPointer inputs, existing code uses:
- `process.ip_content_type(in_msg)` to read content type
- `common.schema_from_content_type_string(...)`
- cast only when schema can be resolved (`string/to_string.py`).

## 5. Migration notes: old `standard` -> new `process`

| Old (`type="standard"`) | New (`type="process"`) |
| --- | --- |
| `defaultConfig={...}` | typed `ProcessConfig` model + `config=Config` |
| `async def run_component(port_infos_reader_sr, config)` | `class X(process.Process[Config])` + `async def run(self)` |
| `p.PortConnector...` / `pc.in_ports[...]` | `self.read_in(...)`, `self.write_out(...)`, `self.in_ports`, `self.out_ports` |
| `p.update_config_from_port(config, pc.in_ports["conf"])` | `await self.update_config_from_port("conf")` |
| `c.run_component_from_metadata(...)` | `process.run_process_from_metadata_and_cmd_args(...)` |

Migration checklist:

1. Convert `defaultConfig` entries into typed `Field(...)` config fields.
2. Keep metadata IDs and descriptions; change `type` to `"process"`.
3. Replace port connector read/write logic with Process methods.
4. Preserve bracket handling and attributes behavior if present.
5. Register command in `configs/local_cmds.json` (ID match required).

## 6. Practical conventions that save rework

- Start from `components/component_templates/process_component_template.py`.
- Keep naming consistent: `METADATA`, `Config`, `Component` or descriptive class name.
- Log start/config-updated/finish consistently.
- Handle missing required config early and return cleanly (`file/read_file.py`).
- For message-level failures, log and continue when safe; avoid crashing whole process if one IP is malformed (e.g. JSON decode issues).

## 7. Notes from `json/filter_json` implementation (recent example)

- Input: JSON string on `in`.
- Config:
  - `traversal_path`: optional tree path to leaf node.
  - `path_separator`: separator token.
  - `filter_paths`: selected fields/paths; supports `alias=path`.
  - `values_only`: optionally output list-of-values instead of objects.
- Behavior:
  - top-level list => apply projection per item.
  - top-level object => project object or recursively apply to nested values.
  - atomic JSON => pass through unchanged.
  - preserves bracket IPs.
- Output: filtered JSON string on `out`.

## 8. What to provide when requesting a new component

To get a complete component in one pass, include:

1. category + component name
2. exact input/output port names and content types
3. config fields (name, type, default, meaning)
4. expected behavior for:
   - malformed input
   - missing config/path/field
   - bracket/substream handling
   - attribute propagation
5. whether array in/out semantics are needed
6. one realistic input/output example

That usually avoids extra iterations and keeps implementation cost low.
