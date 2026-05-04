# FBP components in this folder

This folder contains the local Python implementations of the FBP components that are exposed by this repository.

There are **two component styles** in the codebase:

| Style | Current usage in this repo | Typical example | Runtime helper |
| --- | --- | --- | --- |
| `standard` | dominant (`33` components) | `string/split_string.py` | `zalfmas_fbp.run.components` + `zalfmas_fbp.run.ports` |
| `process` | newer/minority (`2` components) | `string/split_string2.py` | `zalfmas_fbp.run.process` |

If you want to understand how components are usually written here, start with the `standard` pattern. If you want to create a component as an actual `fbp.capnp:Process`, use `split_string2.py` as the primary template.

## How a component becomes available

1. Create a Python module under `zalfmas_fbp/components/<category>/`.
2. Define a top-level `meta` dictionary with category and component metadata.
3. Provide a `main()` entrypoint that delegates to the matching runtime helper.
4. Add the component command to `configs/local_cmds.json`.
5. Start `zalfmas_fbp.run.local_components_service`.

The local component service discovers components by executing their command with `-O`, reading the JSON metadata, caching it, and then exposing the component through a factory:

- `standard` components become a `RunnableFactory`
- `process` components become a `ProcessFactory`

## Common structure most components have

Nearly every component module contains these parts:

### 1. A `meta` dictionary

This is the most important static description of the component. It usually contains:

- `category.id` and `category.name`
- `component.info.id`, `component.info.name`, `component.info.description`
- `component.type` (`standard` or `process`)
- `component.inPorts`
- `component.outPorts`
- optional `component.defaultConfig`

Typical `defaultConfig` entries look like this:

```python
"defaultConfig": {
    "split_at": {
        "value": ",",
        "type": "string",
        "desc": "Split string at this character.",
    }
}
```

### 2. Declared ports

The common port names are:

- `in` for the main input
- `out` for the main output
- `conf` for configuration

Port declarations usually also describe the `contentType`. Some components additionally mark an output as an array port with `"type": "array"`.

### 3. A single async processing loop

Most components:

- connect their ports once at startup
- read messages in a loop
- stop when they receive a `done` message or a disconnect
- create new `fbp_capnp.IP` messages for output

### 4. A tiny `main()` function

The `main()` function is important because the helpers add the common CLI behavior used by the service, especially:

- `-O` / `--output_json_component_metadata`
- `-o` / `--output_json_default_config`
- `-W` / `-w` for writing those files instead of printing them

The local component service depends on `-O`, so a component without the helper-based `main()` will not integrate cleanly.

## The dominant pattern: `standard` components

The classic component style in this repository is a plain async function:

```python
async def run_component(port_infos_reader_sr: str, config: dict):
    ...
```

This is the pattern used by `string/split_string.py`, `file/read_file.py`, `console/console_output.py`, `ip/copy.py`, and most other modules in this folder.

### How it works

1. Connect ports with `PortConnector.create_from_port_infos_reader(...)`.
2. Optionally merge config from the `conf` port with `update_config_from_port(...)`.
3. Read from `pc.in_ports["in"]`.
4. Write `fbp_capnp.IP` messages to `pc.out_ports["out"]`.
5. On shutdown, call `await pc.close_out_ports()`.

### What is important about this variant

- The incoming `config` is a **plain Python dict**.
- The `conf` port is usually the place where JSON/TOML config arrives.
- This style matches the older executable startup flow where the component is launched with a `port_infos_reader_sr`.
- It is the best reference when you want to follow the existing majority style in this repository.

### Minimal skeleton

```python
import zalfmas_fbp.run.components as c
import zalfmas_fbp.run.ports as p


async def run_component(port_infos_reader_sr: str, config: dict):
    pc = await p.PortConnector.create_from_port_infos_reader(
        port_infos_reader_sr,
        ins=["conf", "in"],
        outs=["out"],
    )
    await p.update_config_from_port(config, pc.in_ports["conf"])

    while pc.in_ports["in"] and pc.out_ports["out"]:
        msg = await pc.in_ports["in"].read()
        if msg.which() == "done":
            pc.in_ports["in"] = None
            continue

        ...

    await pc.close_out_ports()


def main():
    c.run_component_from_metadata(run_component, meta)
```

## The focus pattern: `process` components

The `process` variant is the more explicit capability-oriented style. In this repository the key example is `string/split_string2.py`; `string/to_string.py` follows the same overall idea.

This pattern is used when the component should be exposed as an actual `fbp_capnp.Process` implementation instead of only as a runnable executable.

### How it works

Instead of writing a free `run_component(...)` function, you:

1. set `meta["component"]["type"] = "process"`
2. subclass `zalfmas_fbp.run.process.Process`
3. pass the metadata into the base class
4. implement `async def run(self)`
5. start the component with `process.run_process_from_metadata_and_cmd_args(...)`

### `split_string2.py` as the main template

`split_string2.py` shows the essential shape:

```python
import zalfmas_fbp.run.process as process


class SplitString(process.Process):
    def __init__(self, metadata, con_man=None):
        process.Process.__init__(self, metadata=metadata, con_man=con_man)

    async def run(self):
        while True:
            in_msg = await self.read_in("in")
            if in_msg is None:
                break

            ...


def main():
    process.run_process_from_metadata_and_cmd_args(SplitString(meta), meta)
```

### What is important about this variant

- `self.read_in("name")` and `self.write_out("name", value=...)` are the stop-aware accessors for standard ports.
- `self.array_out_ports["name"]` accesses array output ports.
- `self.config` is managed by the base class.
- Config entries are stored as **Cap'n Proto value objects**, not plain Python values. For example, `split_string2.py` reads the delimiter with `self.config["split_at"].t`.
- The base class owns lifecycle transitions, stop handling, and output-port cleanup. `run()` should only implement component work.
- The startup path is different from the `standard` style: the helper expects a `process_cap_writer_sr`, not a `port_infos_reader_sr`.

### Very important difference from the `standard` style

In the `standard` variant, configuration is usually pulled from the `conf` input port by calling `update_config_from_port(...)`.

In the `process` variant, that does **not** happen automatically.

`split_string2.py` still declares a `conf` port in metadata, but its implementation reads the delimiter from `self.config`, not from `self.in_ports["conf"]`. That means:

- declare a `conf` port only if you really plan to consume it
- if you want config from that port, read it explicitly in `run()`
- otherwise rely on the process config entries / defaults exposed by the `Process` base class

### When to use this variant

Prefer the `process` style when the surrounding system expects a long-lived process capability and not just a started executable.

Prefer the `standard` style when you want to match the dominant style in this repository and you only need the usual port-driven runnable behavior.

## Bracket IPs and substreams

The FBP `IP` schema has a lightweight grouping mechanism via `IP.type`:

- `standard` for normal data messages
- `openBracket` to mark the start of a substream
- `closeBracket` to mark the end of a substream

Use this when a component needs to send a logical group of messages in chunks while preserving the group boundary:

```python
await out_port.write(value=fbp_capnp.IP.new_message(type="openBracket", content=group_id))

for item in items:
    await out_port.write(value=fbp_capnp.IP.new_message(content=item))

await out_port.write(value=fbp_capnp.IP.new_message(type="closeBracket", content=group_id))
```

Consumers that understand substreams usually either preserve the bracket IPs or flatten them, depending on config:

```python
in_ip = msg.value.as_struct(fbp_capnp.IP)

if in_ip.type == "openBracket":
    if config["maintain_substreams"]:
        await out_port.write(value=in_ip)
    continue

if in_ip.type == "closeBracket":
    if config["maintain_substreams"]:
        await out_port.write(value=in_ip)
    continue
```

Existing examples:

- `climate/climate_service_to_datasets.py` can create a substream around a climate service's datasets.
- `climate/datasets_to_timeseries.py` can preserve incoming substreams and can create a substream around each dataset's time series.
- `climate/timeseries_cap_to_data.py` can preserve or flatten incoming substreams.
- `geo/create_lat_lon_coords.py` can bracket streamed coordinate output when `stream` and `create_substream` are enabled.

There is no shared helper abstraction for bracketed streams yet; the pattern is implemented directly in components. When writing bracket IPs, make sure every async `write(...)` is awaited.

## A practical checklist for adding a new component

1. Pick the category folder.
2. Decide whether the component should be `standard` or `process`.
3. Create a unique `component.info.id`.
4. Fill `meta` completely.
5. Implement the async logic.
6. Add a helper-based `main()`.
7. Register the command in `configs/local_cmds.json`.
8. Make sure the key in `configs/local_cmds.json` is exactly the same as `meta["component"]["info"]["id"]`.

The last step is critical: the local component service skips a component when the ID from the command mapping and the ID inside the component metadata do not match.

## Which template to start from

- For a new `process` component: start from `string/split_string2.py`
- For a new component following the repo majority: start from `string/split_string.py`
- For array-output behavior: also look at `ip/copy.py`
- For a simple sink: also look at `console/console_output.py`

In short: **most existing components are simple `standard` run loops, but `split_string2.py` is the clearest example if you want to build a real process-based component.**
