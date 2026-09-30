# Legacy components: conversion, tests and consistency

Status: proposal, 2026-09-30. Companion to `base_components_plan.md` (which covered the *new* base
set, now complete through WP6) and `agents_process.md` (how to write a Process component).

The base set is done, so attention moves to what was already there. Three jobs, in one plan because
they interact: converting the remaining Runnable-style components, giving the untested ones
characterization tests, and fixing the inconsistencies those tests will expose.

---

## 1. What a survey of the tree actually found

Measured, not estimated — 87 components in `zalfmas_fbp/components/`.

| | Count |
|---|---|
| `type="process"` | 66 |
| `type="standard"` (Runnable-style) | 21, one of which is the template |
| **No test imports them at all** | **38** — 18 process, 20 standard |

### 1.1 One outright bug

`climate/timeseries_data_to_csv` and `climate/timeseries_data_to_monthly_aggregate` share **the same
`info.id`** (`6b11cf2a-08bb-43f9-964a-1d4ed248cce9`) *and* the same name ("timeseries data -> csv")
and description. `configs/local_cmds.json` is keyed by id, so only one entry can exist:

```
local_cmds entry: python -m zalfmas_fbp.components.climate.timeseries_data_to_csv
monthly_aggregate: not registered — unreachable
```

**`timeseries_data_to_monthly_aggregate` can never be launched.** 143 lines that no flow can reach.
Presumably a copy-paste when it was written; nothing has caught it because nothing tests either.

### 1.2 Patterns worth a sweep

| Pattern | Files | Why it matters |
|---|---|---|
| Blind `except Exception:` | 28 | Hides real failures; a typo inside a `try` looks like bad input |
| `common.copy_and_set_fbp_attrs` | 15 | Drops `desc`, and applies only the *first* matching override — `brackets.copy_attrs` does neither |
| Unguarded `as_struct(common_capnp.Value)` | 2 | D14: a struct that is not a `Value` is silently misread, not rejected — `json/get_value_by_key`, `spotpy/spotpy_comp` |
| Process components not bracket-aware | 11 | D3 says bracket transparency is the default; these drop or mangle substreams |

The bracket-transparency list is the most consequential, since a substream passing through any of
these currently breaks downstream grouping: `ip/add_attribute`, `ip/add_content`, `ip/copy_ip`,
`ip/lift_attributes`, `ip/load_balancer`, `string/split_string2`, `string/to_string`,
`file/write_file`, `models/monica/write_monica_csv`, `dakis/create_empty_raster`, and the process
template itself.

### 1.3 What can actually be tested

The remaining Runnable-style components split sharply, and this drives the ordering more than
anything else:

| Class | Components | Testable how |
|---|---|---|
| **Pure** — no I/O beyond the ports | `console_output`, `split_string`, `collect_into_list`, `ordered_flatten_nested_dicts`, `to_geo_coord`, `timeseries_data_to_csv`, `timeseries_data_to_monthly_aggregate` | Directly, like every WP1–WP6 component |
| **Local resources** — files or geo libraries | `read_csv`, `proj_transform_coordinates`, `create_lat_lon_coords`, `get_lat_lon_grid_value`, `ilr_sowing_harvest_dates` | `tmp_path` fixtures and small fixture data |
| **Capability-dependent** — need a live service | `climate_service_to_datasets`, `datasets_to_timeseries`, `timeseries_cap_to_data`, `use_grid_service`, `use_soil_service`, `create_monica_env` | Only with a fake capability; needs a harness that does not exist yet |
| **Flow-specific** — large, single-purpose | `africa_calibration_producer` (636 lines), `africa_calibration_consumer` | Conversion is mechanical; meaningful tests need the flow's context |

---

## 2. Ordering, and why

The user's instinct — convert first, then test — is right for the Runnable components, with one
exception worth stating.

**Convert before testing** where the component is being converted anyway: writing characterization
tests against the `standard` API and then rewriting them for the Process API is double work, and the
old tests would pin an API that is about to disappear.

**Test before touching** where a component is *not* being converted but *is* being changed — the 18
untested `process` components due a bracket-transparency or attribute-handling fix. There,
characterization tests first is exactly the D12 discipline that caught the `li8`/`lui8` difference
and the `write_file` D14 violation. Both of those would have shipped silently otherwise.

So: convert the Runnables (with tests written once, against the new shape), and separately
characterize-then-fix the existing Process components.

---

## 3. Work packages

### LP0 ✅ — the outright bug, and a guard against it recurring

Done.

- `climate/timeseries_data_to_monthly_aggregate` **deleted**. It had been unlaunchable, and
  maintaining it cost more than it was worth.
- `string/split_string` (Runnable) **deleted**; `string/split_string2` **renamed** to
  `split_string`, keeping its own id and gaining the plain name. `flows/test_flow4.json` referenced
  the retired id and now points at the survivor — the configs were identical, so nothing else
  changed.
- `string/collect_into_list` **retired**, superseded by `json/concat_json_substream`.
- `tests/run/test_component_registry.py` added: ids and names unique tree-wide, every registered
  command's module exists, every registered id matches its module's `METADATA`, the cache covers the
  registry, and every id is a UUID. Templates are excluded — their placeholder metadata is
  deliberate.
- `zalfmas_fbp/components/README.md` **rewritten**. It called `process` a "minority (2 components)",
  pointed at the file that was just renamed as the template to copy, and taught
  `update_config_from_port`, which is now a no-op. It duplicated `agents_process.md`, which is how it
  drifted, so it is now a map to the authoritative documents rather than a third copy of them.

### LP1 — characterization tests for the pure Runnables, then convert them

`console_output`, `split_string`, `collect_into_list`, `ordered_flatten_nested_dicts`, `to_geo_coord`.

These are small, pure, and the conversion is mechanical. Write the test against the *converted*
component, but convert and test in one commit per component so a regression is attributable.

`string/split_string` is worth a decision rather than a conversion: `string/split_string2` already
exists as its Process replacement. Either retire the old one or, if flows still reference its id,
keep it as a thin alias. Same question for `collect_into_list` versus `json/concat_json_substream`.

### LP2 — the local-resource Runnables

`read_csv`, `proj_transform_coordinates`, `create_lat_lon_coords`, `get_lat_lon_grid_value`,
`ilr_sowing_harvest_dates`.

Conversion plus tests using `tmp_path` and small committed fixtures. `read_csv` deserves attention
beyond conversion: it emits raw text and nothing parses it, so it is also the natural place to
revisit the P2 `file/csv_to_json` idea — but only if a flow wants it.

### LP3 — a fake-capability harness, then the service-dependent Runnables

`climate_service_to_datasets`, `datasets_to_timeseries`, `timeseries_cap_to_data`,
`use_grid_service`, `use_soil_service`, `create_monica_env`.

These need something the test harness does not have: a way to stand up a fake Cap'n Proto capability
and hand it to a component over a port. That harness is the real deliverable here — it unblocks
testing every service-using component, including the `dakis` ones. Worth building once, carefully.

Until it exists these components can be *converted* but only smoke-tested (metadata, config
validation, clean start and stop with no input).

### LP4 — bracket transparency for the 11 Process components

Characterization tests first, then `brackets.handle_bracket` with an explicitly stated policy, per
D3. Expect this to change behaviour for substreams in every one of them — that is the point, but it
means each needs a test showing what it did before and a note saying what it does now.

`ip/copy_ip` and `ip/load_balancer` need thought rather than a mechanical fix: what a *broadcast* or
a *distribution* should do with a bracket pair is a semantic question, not an oversight.
`route_ips` (WP3) already had to answer it — brackets broadcast to every output, so each branch sees
well-formed substreams — and `copy_ip` should probably match.

### LP5 — the consistency sweep

Once the above have tests, the mechanical fixes become safe:

- `copy_and_set_fbp_attrs` → `brackets.copy_attrs` in the 15 files, gaining `desc` preservation and
  all-overrides behaviour.
- Blind `except Exception:` narrowed to what can actually be raised, in the 28 files.
- The two unguarded `Value` casts driven from `valueType` via `values.python_from_attr`.
- The process template updated to show bracket handling, since it is what new components are copied
  from.

### LP6 — dropped

`africa_calibration_producer` and `africa_calibration_consumer` are being deleted or moved out of
this repository, so they are not converted.

---

## 4. Definition of done, per converted component

1. `type="process"`, typed `Config(process.ProcessConfig)`, no `conf` or `log` declaration (WP-1).
2. Ports carry explicit `role` and `required`.
3. Bracket policy explicitly chosen and stated in the metadata description.
4. Attributes via `brackets.copy_attrs`; values written as `common.Value` with `valueType` (D4).
5. Reads driven by an explicit type, never a guessed cast (D14).
6. Test under `tests/components/<category>/`, covering happy path, malformed input, missing config,
   bracket handling and attribute propagation.
7. Registered in `configs/local_cmds.json` under an id that is unique tree-wide, and the components
   cache regenerated.

## 5. Open questions for the user

Answered 2026-09-30:

1. `timeseries_data_to_monthly_aggregate` — **deleted**.
2. `split_string` — old one **deleted**, `split_string2` renamed over it. `collect_into_list` —
   **retired**.
3. The africa calibration producer and consumer — **leave them alone**; they will be deleted or
   moved elsewhere. LP6 is dropped.
4. **LP4 before LP3**, as recommended.

Standing direction: domain-specific components should leave this repository mid-term — the `dakis`
set is the obvious candidate. What is missing is a user-friendly mechanism for working with several
component libraries or services at once, so this waits on that rather than on effort. It does mean
new work should not deepen the coupling between the base set and any one domain.
