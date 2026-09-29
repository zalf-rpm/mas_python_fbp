# Process-style component notes for `mas_python_fbp`

This file summarizes practical rules and implementation hints from existing **Process-style** components under `zalfmas_fbp/components/` (for example `string/split_string2.py`, `string/to_string.py`, `models/monica/create_monica_capnp_env.py`, `json/update_json.py`, `ip/*`).

The goal is to make creating new components (or migrating old `standard` components) faster and more consistent.

## 1. Canonical Process-style structure

Use this shape every time:

1. Define a typed config model:
   - `class Config(process.ProcessConfig): ...` with `pydantic.Field(...)`.
2. Define `METADATA = meta.Component(...)`:
   - `type="process"`
   - explicit `inPorts` / `outPorts`, **without `conf` or `log`** - those are runtime-owned and
     injected automatically (see §3.1)
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
- Keep `contentType` and port naming aligned with behavior (`in`, `out`, plus domain ports).
- Give every declared port an explicit `role` and `required`. Reserved names get their role for
  free: `err` -> `error`, `rej` -> `reject`. `conf` and `log` are runtime-owned and must not be
  declared at all; declaring one is redundant, and claiming its role on another port is rejected.
- For multi-target output, mark out port as array (`type="array"`), e.g. `ip/copy_ip.py`, `ip/load_balancer.py`.

## 3. Typical `run()` loop patterns

### 3.1 Config: nothing to do

The runtime owns the `conf` port. It applies the initial config **before** `run()` is called, and
applies later updates at IP boundaries - immediately before `read_in` hands an IP over, never part
way through processing one. So:

- Do **not** declare a `conf` port; it is injected into the metadata.
- Do **not** call `update_config_from_port`; it is a deprecated no-op.
- Read `self.config` wherever it is needed. Snapshotting it before the loop is still correct, since
  a change can only land between IPs - but a component that wants to react to updates should read
  `self.config` inside the loop instead.

Likewise every Process gets a runtime-owned `log` out-port. Logging normally with
`logger.info(...)` is all a component does; when a flow connects `log`, the runtime mirrors those
records onto it as `fbp.capnp:LogMessage` IPs, with the local logger still in use alongside.

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

**Bracket transparency is the library default**: a component that does not reason about substreams
forwards bracket IPs unchanged. Use `components/common/brackets.py` rather than hand-rolling it:

- `BracketPolicy` states the intent, `handle_bracket(ip, policy, write)` performs it.
- `collect_substream(read, open_ip)` reads one complete, possibly nested substream and returns a
  `Substream` tree (`is_leaf`, `ips`, `all_ips()`, `leaves()`), with `truncated` set if the input
  closed early. Do not count nesting levels by hand.
- `BracketTracker` counts depth for components that only need to know where they are.

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

Prefer `components/common/brackets.py`'s `copy_attrs(source, target, extra=..., remove=...)` and
`set_attrs(ip, attrs)`: they preserve `desc` as well as `valueType`, apply every override rather than
only the first, and only write optional Text fields that are actually set - reading an unset one
yields `""`, and writing that back turns "unset" into "explicitly empty".

**Settled AnyPointer rule for attributes (D4):** base components **always write `common.Value` with
`valueType` set**, including for strings. `set_attrs` does this for plain Python values. On the
reading side, `values.python_from_attr` accepts both a typed `Value` and a raw Text attribute, so
older components keep working; that tolerance lives in one place, not in every component.

This is a correctness requirement, not tidiness - see §4.4.

### 4.4 Content typing / schema detection

**Never guess a schema (D14).** Cap'n Proto validates a pointer's *kind* (text vs. struct) but not
*which* struct it holds, so casting a struct pointer to the wrong schema does not raise - it
silently reinterprets the bits. Reading a `StructuredText` as a `Value` yields `f64 = 5e-324`, not an
error. A "try `Value`, fall back" chain is therefore unsound and must not be written.

Use `components/common/values.py`, which drives every read from an explicit type - the attribute's
`valueType`, the IP's `sysAttributes.contentType`, or one from component config - and returns
`MISSING` rather than a guess:

- `resolve_schema(content_type)` (cached), `python_from_content(ip, fallback_type)`,
  `python_from_attr(kv, type_hint)`
- `json_from_capnp` / `capnp_from_json` for whole structs, `value_from_python` /
  `python_from_value` for `common.Value`

`as_text()` *does* raise for a struct pointer, so it is a sound probe for "text or struct" - that is
the one discrimination the library relies on.

## 5. Migration notes: old `standard` -> new `process`

| Old (`type="standard"`) | New (`type="process"`) |
| --- | --- |
| `defaultConfig={...}` | typed `ProcessConfig` model + `config=Config` |
| declared `conf` port | runtime-owned, do not declare |
| `async def run_component(port_infos_reader_sr, config)` | `class X(process.Process[Config])` + `async def run(self)` |
| `p.PortConnector...` / `pc.in_ports[...]` | `self.read_in(...)`, `self.write_out(...)`, `self.in_ports`, `self.out_ports` |
| `p.update_config_from_port(config, pc.in_ports["conf"])` | nothing - the runtime applies config itself |
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
