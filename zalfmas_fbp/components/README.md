# FBP components in this folder

Each module here is one component: a `METADATA` block describing it, a typed config model, and a
class implementing `run()`. A component is only launchable if `configs/local_cmds.json` maps its
`info.id` to a module command, and the flow editor sees it through
`configs/local_components_cache.json`, regenerated with `pixi run regenerate-components-cache`.

## Which style to write

**Write `type="process"` components.** They are the supported style and the large majority of what
is here. `type="standard"` is the older Runnable-based style, kept only until the remaining ones are
converted — see `legacy_components_plan.md` at the repo root.

Start from `component_templates/process_component_template.py`.

## Where the rules are written down

This file is a map, not a manual, so that there is one place per subject rather than three that
drift apart:

| Subject | Read |
|---|---|
| How to write a Process component: structure, ports, config, brackets, attributes, content typing | `agents_process.md` (repo root) |
| Why the base components are shaped the way they are, and the decisions behind them (D1–D16) | `base_components_plan.md` (repo root) |
| Converting the remaining Runnable components, and their test gaps | `legacy_components_plan.md` (repo root) |

## Things that are easy to get wrong

Each of these is a decision with a reason, not a style preference. `agents_process.md` explains all
of them; they are listed here because they are the ones that bite.

- **Do not declare `conf` or `log` ports.** The runtime owns them and injects them into the
  metadata. It applies config before `run()` starts and between IPs thereafter, so a component never
  reads `conf` itself. `Process.next_config()` exists for a component with no data input that wants
  to be driven by its config.
- **Forward bracket IPs unchanged** unless the component genuinely reasons about substreams. Use
  `common/brackets.py` rather than hand-rolling nesting counts.
- **Never guess a Cap'n Proto type.** Casting a struct pointer to the wrong schema does not raise —
  it silently reinterprets the bits. Read through `common/values.py`, which drives every read from an
  explicit type and returns `MISSING` rather than a guess.
- **Write attribute values as `common.Value` with `valueType` set.** `common/brackets.py`'s
  `set_attrs`/`copy_attrs` do this, and preserve `desc` and every override, which
  `zalfmas_common.common.copy_and_set_fbp_attrs` does not.
- **`info.id` must be unique across the whole tree.** A duplicate makes one of the two components
  unlaunchable, since `local_cmds.json` is keyed by id. A test enforces this.

## Shared helpers

`common/` holds what components are built from, rather than components:

- `selectors.py` — the selector and predicate language (`@attr`, `./path`, `#type`)
- `values.py` — Cap'n Proto ↔ Python ↔ JSON conversion, both directions
- `brackets.py` — substream collection, bracket policy, attribute copying
- `templating.py` — rendering a string from an IP (`{@attr}`, `{count}`, `{now:%Y-%m-%d}`)
