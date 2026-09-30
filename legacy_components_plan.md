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

### LP1 ✅ — the pure Runnables

`console_output`, `to_geo_coord`, `ordered_flatten_nested_dicts` — `split_string` and
`collect_into_list` were retired in LP0 rather than converted.

Done, one commit each, 45 tests where there had been 1. None of the three was purely mechanical:

- **`console_output`** printed bracket IPs as blank lines and fell back to `repr` for typed content.
  It now renders content through its declared type, reports unreadable content rather than guessing
  (D14), skips brackets unless asked, and can show attributes, type and a running count — enough to
  serve as a quick console probe.
- **`to_geo_coord`** rejected every capitalised coordinate name it documented.
  `zalfmas_common.geo.name_to_struct_type` lowercases when matching `2d`/`xy`/`latlon` but compares
  the raw string for `wgs84`, `gk*` and `utm*`, so `LatLon` worked while `WGS84`, `GK5` and `UTM32N`
  returned `None` — and the old code then crashed on `None.copy()`. Normalising the name in the
  component fixes all of them without an upstream change. It also accepts `common.Value` lists and
  JSON text now, which is what `sequence`, `json_to_common_value` and `split_json` emit.
- **`ordered_flatten_nested_dicts`** had a `default_config` dict at module level that was never
  wired into its `METADATA`, so `config["reverse"]` raised `KeyError` for every IP — swallowed by a
  bare `except`, silently dropping the whole stream when unconfigured.

Each has a typed config, bracket transparency and attribute propagation, none of which they had.

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

### LP4 ✅ — bracket transparency for the 11 Process components

Done. Characterization first paid for itself twice over.

**One was a false positive.** `ip/copy_ip` was already correct: broadcasting every IP including
brackets is exactly what keeps a substream intact on each output. The survey had flagged it by
grepping for `"openBracket"`, which cannot see correctness that comes from treating everything
uniformly. It now says so in a comment, so the next reader does not mistake it for an oversight.

**Two were worse than "brackets pass through wrong".** `ip/add_attribute` and `ip/add_content` read
their *second* input before checking what arrived on `in`, so a bracket consumed an IP from the
`attr`/`content` port and a substream slipped the pairing — the open bracket and the first data IP
ate both partners, and the second data IP got whatever was left. `ip/lift_attributes` rebuilt every
IP with `new_message(content=...)`, which drops the `type`, so a bracket came out as a *standard* IP
and the substream was destroyed outright.

**The two sinks produced spurious files.** `file/write_file` and `models/monica/write_monica_csv`
wrote a file for every bracket and advanced the running count used in filename patterns, so
`{count}` was wrong for everything after the first substream.

`string/split_string` forwards incoming brackets and gained `wrap_in_substream` (off by default) per
the user's specification: the default keeps the flat shape it has always produced, which is also
what arrives when the strings come in separately. `string/to_string` and `dakis/create_empty_raster`
forward brackets rather than emitting a standard IP for them.

`ip/load_balancer`'s description now states that a substream is one unit of work, and what to do if
the IPs inside one should be parallelised: strip the brackets before it and reassemble after. That
is what the `amei_exercises` calibration flow does deliberately — keeping the brackets would send a
whole substream to a single worker — so it is a documented consequence, not a workaround.

Every component's bracket policy is now stated in its metadata description, and the process template
demonstrates the default.

`ip/copy_ip` and `ip/load_balancer` needed a semantic decision rather than a mechanical fix, and it
has been made: **`copy_ip` keeps a substream intact on every output** (as `route_ips` does), and
**`load_balancer` treats a whole substream as one unit of work**, routing it entirely to one output.
Tearing a group across workers leaves every branch with a malformed stream; the calibration flow in
`amei_exercises` works around it today by stripping brackets before the balancer and reassembling
after, which this removes the need for.

Characterization showed `copy_ip` was **already correct** — it broadcasts every IP including
brackets, so the survey's text-based flag was a false positive. `load_balancer` was not: a nested
substream came out as `[['openBracket','openBracket','closeBracket'], ['standard','standard','closeBracket']]`.

Routing a sequence to one chosen slot needed `Process.choose_array_out_index`, since the
`write_array_out` strategies choose per message. Third such accessor after `write_array_out_at` and
`read_array_in_with_index`, each added because a component needed it.

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
