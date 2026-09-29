# Base component algebra — gap analysis and implementation plan

Status: proposal, 2026-09-29. Companion to `agents_process.md` (which describes *how* to write a
Process component; this document describes *which* ones are still missing and why).

---

## 1. The model: six axes of a data-flow algebra

A component library is "complete enough" when, for each axis below, the primitive operations exist
*and* each operation has its inverse. Most of the gaps in this repo are missing inverses.

| # | Axis | Question it answers | Primitives |
|---|------|--------------------|------------|
| A | **Stream cardinality** | Which IPs go where, how many are there? | filter, route, merge, copy, gate, take/drop, dedupe |
| B | **Stream structure** | How are IPs grouped into substreams? | wrap, unwrap/flatten, group-by, reduce, sort, join |
| C | **IP anatomy** | Content vs. attributes — what lives where? | content→attr, attr→content, add/remove/rename attrs, lift |
| D | **Representation** | Cap'n Proto ↔ JSON ↔ Text ↔ bytes | to/from JSON, to/from Value, to/from struct, content-type tagging |
| E | **Value transforms** | Within one representation, change the data | update/patch, map, project/filter fields, aggregate, interpolate, format |
| F | **Edges** | Where does data enter and leave? | file/dir/excel/csv/object-store readers & writers, generators, sinks, probes |

### Current coverage

| Axis | Have | Missing (the gaps) |
|---|---|---|
| A | `copy_ip`, `copy_ip_on_trigger`, `load_balancer` | **filter**, **route/switch**, **merge (array-in → out)**, gate, take/skip/every-nth, dedupe |
| B | `wrap_into_substream`, `split_bracketed_stream`, `sort_ips`, `concat_json_substream` | **group-into-substreams**, **flatten substreams**, **generic reduce over a substream**, **join/correlate by key** |
| C | `add_attribute`, `add_content`, `attribute_to_content`, `remove_attributes`, `lift_attributes` | **content→attributes (path extraction)**, **attributes→content (as JSON object)**, rename/keep/default attrs |
| D | `json_to_common_value`, `to_string` | **capnp struct → JSON**, **JSON → capnp struct**, **Value → JSON**, content-type set/normalise, StructuredText wrap/unwrap, bytes↔text |
| E | `update_json`, `map_json_values`, `filter_json`, `apply_gjson_queries`, `get_value_by_key`, `interpolate_json_by_key`, `split_string(2)`, `collect_into_list`, `ordered_flatten_nested_dicts` | **split JSON array → stream**, **merge/deep-patch JSON**, **format/template string**, sort JSON, aggregate JSON, validate JSON, regex, CSV↔JSON |
| F | `read_file`, `read_csv`, `read_excel`, `write_file`, `console_output`, `counter`, dakis object-store/disk IO | **sequence/range generator**, **probe (pass-through log)**, discard sink, list/glob files, end-of-stream trigger |

**Headline conclusion.** Axis D (Cap'n Proto ↔ JSON) is *one-directional today* — you can get into
JSON and into `common.Value`, but there is no generic way back out to a typed Cap'n Proto struct, and
no way to turn an arbitrary struct into JSON (`to_string` gives you Cap'n Proto's text format, not
JSON). Axis A has no semantic routing at all — `load_balancer` distributes by availability, never by
content. Axis B can create and consume substreams but cannot *derive* them from data (group-by) or
collapse them generically (reduce). Those three are the load-bearing gaps; everything else is
convenience.

---

## 2. Do the shared foundations first

Roughly a dozen of the missing components need the same two or three capabilities. Implementing
those as component-level helper modules first makes each component small, consistent, and testable —
and avoids a repeat of the situation where every component re-implements bracket handling and
attribute copying slightly differently.

### S1 — `zalfmas_fbp/components/common/selectors.py` (selector + predicate mini-language)

One way to say "this piece of this IP", used by filter, route, group-by, dedupe, sort, join,
reduce, format-string, content→attr.

Recommended syntax — it extends what `update_json` and `write_file.filename_pattern` already do:

```
@attr                 -> the IP attribute named "attr" (coerced to a Python value)
@attr/sub/0/path      -> path into the attribute's value (capnp struct field, Value, or JSON)
.                     -> the IP content itself
./a/b/0               -> path into the content (JSON object/array, capnp struct field, or Value)
#type                 -> the IP type ("standard" | "openBracket" | "closeBracket")
#contentType          -> sysAttributes.contentType
"literal"             -> a literal string; bare numbers/true/false/null are literals
```

**Settled (D1, §7.1):** the `/`-path form above is the default — it matches every existing
component's `path_separator` config — with an **opt-in** GJSON query via a `gjson:` prefix for JSON
content (`gjson` is already a dependency and `apply_gjson_queries` already uses it). GJSON is not the
default: it does not apply to Cap'n Proto content, and half these components must work on both.
Anything not starting with a sigil is a literal; a literal starting with one is escaped as `\@`.

Predicate form (JSON/TOML-friendly, so it fits the `conf` port):

```toml
[[predicates]]
left = "@region"
op   = "eq"          # eq ne lt le gt ge in not_in contains startswith endswith matches exists missing is_null truthy
right = "brandenburg"
```

Plus `all`/`any`/`not` combinators for nesting. Comparisons coerce permissively (D8), with a
`strict_types: bool = False` opt-out. A component taking a single selector names the field
`selector` (D10).

Deliverables: `parse_selector` (including leading-`\` unescaping), `Selector` and `Predicate` pydantic
models, `resolve(ip, selector) -> Any | MISSING`, `evaluate(ip, predicate) -> bool`, `apply_path`,
and `compare`/`coerce_pair`. The tolerant attribute reader `python_from_attr` (D4) lives in S3, which
S1 imports — **S1 therefore depends on S3's read side and must be built after it.**

### S2 — `zalfmas_fbp/components/common/brackets.py` (substream policy)

Today every component hand-rolls `if in_msg.type in ("openBracket", "closeBracket"): forward`. Give
that a name and one implementation:

- `BracketPolicy.TRANSPARENT` — forward bracket IPs unchanged, apply logic to standard IPs only
  (the default for all map-like components).
- `BracketPolicy.AWARE` — the component gets `on_open` / `on_close` callbacks and tracks nesting
  depth (reduce, sort, concat, group).
- `BracketPolicy.IGNORE` — drop brackets (flatten).
- `SubstreamCollector` helper: reads one complete, possibly nested substream from a port and returns
  `(open_ip, [ips], close_ip)`, correctly counting nesting — the logic currently duplicated in
  `wrap_into_substream`, `sort_ips` and `copy_ip_on_trigger`.
- `copy_attrs_with(ip, **extra)` / `attrs_as_dict(ip)` wrappers that preserve the "only copy Text
  fields if `_has()`" rule discovered in `split_bracketed_stream`.

### S3 — `zalfmas_fbp/components/common/values.py` (representation bridge)

The conversion knowledge is currently spread over `json_to_common_value.py`,
`run/process/config/config_codec.py`, `ip/attribute_to_content.py` and `string/to_string.py`. Extract
and share:

- `python_from_any(reader, content_type=None) -> Any` — AnyPointer → Python, resolving the schema
  from `sysAttributes.contentType`, a config override, or a `Value`/`StructuredText` probe.
- `json_from_capnp(reader, schema) -> Any` — struct reader → plain dict/list (pycapnp `to_dict`,
  plus enum/union/Data/Text normalisation and a policy for capability fields).
- `capnp_from_json(obj, schema) -> builder` — the inverse, with clear errors for unknown fields.
- `value_from_python` / `python_from_value` for `common.Value` (lift the existing type-selection
  logic out of `json_to_common_value` so both directions share it).
- `resolve_schema(content_type_string)` — thin wrapper over `common.schema_from_content_type_string`
  with caching and the `"Text"` / `"AnyPointer"` special cases already handled in `to_string`.

**Constraint D14, established while building S3 — reads must be type-driven, never guessed.**

Cap'n Proto validates a pointer's *kind* (text vs. struct) but not *which* struct it holds. Casting a
struct pointer to the wrong struct schema therefore does not raise — it silently reinterprets the
bits:

```python
from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp

ip = fbp_capnp.IP.new_message(
    content=common_capnp.StructuredText.new_message(type="json", value='{"a":1}')
)
v = ip.as_reader().content.as_struct(common_capnp.Value)   # wrong schema
v.which()   # -> 'f64'   (no exception)
v.f64       # -> 5e-324
```

The struct must travel through an `AnyPointer` field for this to arise — which is exactly how
`IP.content` and `IP.KV.value` work, so it is the normal case here, not an exotic one. A direct cast
on a typed struct reader will not show it (a struct reader has no `.as_struct`).

It is silent because of Cap'n Proto's forward-compatibility rule: reads past the end of a struct's
allocated data section return defaults rather than erroring. `StructuredText.type = json` is
enumerant 1 in bits `[0,16)`; `Value`'s union tag at bits `[64,80)` is past the end and reads 0,
selecting the `f64` branch; `Value.f64` at bits `[0,64)` then reads that 1 as a double bit pattern,
which is `5e-324`. Every number is derivable — the failure mode is deterministic, not corrupt, which
is precisely what makes it dangerous to rely on.

Where the boundary falls:

| Cast | Result |
|---|---|
| Text pointer → `as_struct(Value)` | raises `KjException` |
| struct pointer → `as_text()` | raises `KjException` |
| struct pointer → `as_struct(StructuredText)` (right schema) | correct |
| struct pointer → `as_struct(Value)` (wrong schema) | **silent nonsense** |

So a "try `Value`, fall back" heuristic is unsound and must not be written, while an `as_text()`
probe *is* sound for telling "this is text" from "this is some struct". Every read is driven by an
explicit type — the attribute's `valueType`, the IP's `sysAttributes.contentType`, or a type from
component config — and otherwise returns `MISSING`.

This is also what makes D4's "always write `valueType`" a correctness requirement rather than
tidiness: an attribute holding a struct with no `valueType` is genuinely unreadable.
`copy_and_set_fbp_attrs` preserves `valueType` across copies and accepts a `(value, valueType)` tuple
for writing typed attributes — verified against the installed `zalfmas_common`.

Per D4, `values.py` also owns `python_from_attr` — the single place tolerating both `common.Value`
and raw Text attribute values on read, while every writer emits `common.Value` with `valueType` set.

Per D12, `json_to_common_value` and `to_string` are refactored onto S3 within WP0 (characterization
tests first), which is what validates the abstraction. The three new conversion components in §3 then
become ~80 lines each.

---

## 3. P0 — the twelve components that close the real gaps

Each card is written in the format `agents_process.md` §8 asks for, so an implementer can take one
card and produce the component without further questions. IDs are pre-generated UUID4s; use them
verbatim and register each in `configs/local_cmds.json`.

Conventions applying to all of them unless the card says otherwise:
- `type="process"`, typed `Config(process.ProcessConfig)`, `config=Config` in metadata.
- **`conf` and `log` are runtime-owned (§6) and must not be declared or read by the component.** The
  `conf` rows still shown in the cards below are informational — they document that the component is
  configurable, not that it declares a port. Config is read from `self.config` at any point in
  `run()`; updates land at IP boundaries.
- Optional non-primary ports carry an explicit role: `rej` → `role = reject`, `err` → `role = error`,
  `trigger`/`brackets` → `role = control`. Primary `in`/`out` are `role = data`, `required = true`.
- Bracket policy TRANSPARENT (forward bracket IPs unchanged).
- Attributes propagated via `common.copy_and_set_fbp_attrs`.
- Per-IP failures log a warning and are governed by an `on_error: Literal["skip","pass_through","fail"]`
  config field (default `"skip"`), never crashing the process.
- Every component gets a unit test under `tests/components/<category>/` using
  `tests/component_harness.py:run_process_component`.

---

### P0-1 `ip/filter_ips` — id `3e52dd18-7369-4701-9a9a-7102e7fae1e2` — **implemented**

*Category* `ip` · *Name* "Filter IPs"

| Port | Dir | Content type | Meaning |
|---|---|---|---|
| `conf` | in | `common.capnp:StructuredText[JSON\|TOML]` | config |
| `in` | in | `AnyPointer` | IPs to test |
| `out` | out | `AnyPointer` | IPs where the predicate holds |
| `rej` | out | `AnyPointer` | IPs where it does not (optional; if unconnected, they are dropped) |

Config: `predicate: Predicate` (S1), `invert: bool = False`,
`drop_empty_substreams: bool = True` (if a substream ends up with zero standard IPs, suppress its
bracket pair rather than emitting an empty substream), `on_error` as above.

Behaviour: standard IPs are evaluated and forwarded to `out` or `rej`. Bracket IPs are buffered when
`drop_empty_substreams` is true, otherwise forwarded immediately. A predicate that cannot be
evaluated (selector missing) counts as `False` and is logged at debug level.

Example: `predicate = {left = "./yield", op = "gt", right = 5.0}` with content `{"yield": 7.1}` →
`out`.

*As built:* `drop_empty_substreams` applies to **both** outputs independently, via a bracket gate per
port, so `out` and `rej` each carry well-formed substreams containing only the IPs that went their
way and neither emits one that turned out empty. Nesting is preserved: an empty inner substream is
dropped while its parent survives.

---

### P0-2 `ip/route_ips` — id `d0de1b9f-8f28-4a6c-85c5-8ec0e748d5e4` — **implemented**

*Category* `ip` · *Name* "Route IPs"

The semantic counterpart to `load_balancer`. Ports: `conf`, `in`; out `out` (**array**) and
`default`. Config: `routes: list[Predicate]` — index *i* of the array out-port receives IPs matching
`routes[i]`; `first_match_only: bool = True` (set false for broadcast-to-all-matching semantics);
`default_to_first_output: bool = False`. Unmatched IPs go to `default`, or are dropped if it is
unconnected. Bracket IPs are broadcast to every connected output so each branch sees well-formed
substreams.

Needs an explicit per-index write rather than an `ArrayOutStrategy`. `OutputRuntime` already has
`write_array_out_port(name, index, port, message)`; it is just not exposed on `Process`, so add a
thin `Process.write_array_out_at(name, index, message)` wrapper (resolving the port from
`array_out_ports[name][index]` and returning `False` for a disconnected slot).

*As built:* routes beyond the number of connected slots are ignored with a warning rather than
treated as an error, since the slot count is a property of the flow rather than of the config.
`default_to_first_output` was dropped: `default` covers it, and a silent fallback to slot 0 makes a
misconfigured route hard to notice.

---

### P0-3 `ip/merge_ips` — id `ea8826e7-3c60-42f6-bfca-f0a6285a7a63` — **implemented**

*Category* `ip` · *Name* "Merge IPs"

The inverse of `copy_ip` / `load_balancer`, and the missing half of the array-port story (the
runtime already has `read_array_in` with `ZIP` and `NEXT_AVAILABLE`; no component exposes it).

Ports: `conf`, `in` (**array in**); out `out`. Config:
`strategy: Literal["next_available","round_robin","zip"] = "next_available"`,
`zip_mode: Literal["substream","attributes","json_object"] = "substream"` (only for `zip`:
emit the zipped IPs wrapped in a substream, or fold inputs 1..n into attributes of input 0, or
combine them into one JSON object keyed by port index/name),
`tag_source_attr: str | None = None` (record the originating input index in that attribute),
`close_when: Literal["all","any"] = "all"`.

*As built:* `round_robin` was dropped. The runtime offers no round-robin *input* strategy, and one
would block on each slot in turn, so a single slow source stalls the merge — exactly what
`next_available` avoids. `close_when` was dropped too: the semantics fall out of the strategy rather
than being separately configurable, and saying so is more honest than a knob with one real setting.
`next_available` drains every input and finishes when all have closed; `zip` finishes as soon as any
one does.

`tag_source_attr` needs the slot an IP arrived on, which `read_array_in` discards, so
`Process.read_array_in_with_index` was added alongside `write_array_out_at`. Note that `zip` rejects
bracket IPs at the runtime level, so zip mode does not accept substreams on its inputs.

The `fbp.capnp` comment claiming only an out port may be an array port was corrected in WP-1 (D5).

---

### P0-4 `convert/capnp_to_json` — id `181b4a79-d1c0-4f19-90b8-26191806ae78` — **implemented**

*Category* `convert` · *Name* "Cap'n Proto to JSON"

**The single highest-value missing component.** Ports: `conf`, `in` (`AnyPointer`); out `out`
(`Text (JSON)`). Config: `content_type: str | None = None` (fallback when the IP carries no
`sysAttributes.contentType`; the IP's own type wins, matching `to_string`'s precedence rule),
`indent: int | None = None`, `include_attributes: bool = False` (if true, emit
`{"content": ..., "attributes": {...}}` instead of the bare content),
`enums_as: Literal["name","number"] = "name"`, `data_as: Literal["base64","hex","list"] = "base64"`,
`capabilities: Literal["skip","sturdy_ref","error"] = "skip"`,
`traversal_path: str | None`, `path_separator: str = "/"` (project to a sub-struct first — same
config names as the other JSON components).

Also sets `sysAttributes.contentType = "Text (JSON)"` on the outgoing IP.

Behaviour: unresolvable schema → `on_error` policy. `common.Value` and `common.StructuredText`
inputs get dedicated shortcuts (unwrap `StructuredText` of type JSON rather than double-encoding).

*As built:* `capabilities` is `unresolved: "drop"|"null"|"repr"`, covering any pointer with no
recoverable type rather than capabilities alone. `enums_as` is not implemented — `to_dict()` yields
enumerant names and emitting numbers would mean walking the schema for little gain. Gained
`parse_text_as_json: bool = False`: plain Text content is otherwise emitted as a JSON *string*, so
piping the JSON components here (which emit untyped Text) straight into this one would encode
twice. Off by default, because a text payload that merely looks like JSON would change meaning —
the string `"123"` becoming the number `123`.

---

### P0-5 `convert/json_to_capnp` — id `43d107ff-c1b5-4622-a050-3445b8f7c159` — **implemented**

*Category* `convert` · *Name* "JSON to Cap'n Proto"

The inverse. Ports: `conf`, `in` (`Text (JSON)`), optional `type` in-port (receive the target content
type at runtime as Text); out `out` (`AnyPointer`), optional `err` out (the original JSON for IPs
that failed to convert). Config: `content_type: str` (required, e.g.
`"@0xa4b1a2ad9a77fdc7 = model/monica/sim_setup.capnp:Setup"`), `traversal_path`, `path_separator`,
`unknown_fields: Literal["error","ignore"] = "error"`,
`missing_fields: Literal["default","error"] = "default"`,
`coerce_numbers: bool = True` (accept `"5"` for an int field, and int for a float field).

Sets `sysAttributes.contentType` to the configured type on the outgoing IP. Together with P0-4 this
makes the round trip JSON → struct → JSON the documented way to move between the two worlds, and
lets a flow drop into JSON for a couple of steps and return to typed messages afterwards.

*As built:* `missing_fields` is not implemented — Cap'n Proto already gives unset fields their
declared defaults, so there is nothing to choose. The `type` port carries `role = control`.

A content type naming a *file* rather than a struct (`fbp/fbp.capnp` instead of
`fbp/fbp.capnp:IP` — an easy slip, since the file id sits at the top of the file) resolves to the
file's schema and used to fail deep inside pycapnp with "Cannot convert _Schema to _StructSchema".
`values.capnp_from_json` now rejects it with a message naming the problem.

Generalises `json_to_common_value` (which stays as the specialised, type-optimising path for
`common.Value`); consider giving `json_to_capnp` a `content_type = "…common.capnp:Value"` shortcut
that delegates to the existing logic in S3.

---

### P0-6 `json/split_json` — id `205dfc0a-7283-4322-a1b7-4f952abffb88` — **implemented**

*Category* `json` · *Name* "Split JSON"

The exact inverse of `concat_json_substream`, and the only way to turn one JSON document into a
stream — currently impossible. Ports: `conf`, `in` (`Text (JSON)`); out `out` (`Text (JSON)`).
Config: `traversal_path: str | None`, `path_separator: str = "/"`,
`mode: Literal["list_items","object_values","object_entries"] = "list_items"`,
`wrap_in_substream: bool = True` (emit an open/close bracket pair around the emitted items),
`key_attr: str | None = None` (for object modes, attach the key as this attribute),
`index_attr: str | None = None`, `count_attr: str | None = None` (put the item count on the close
bracket, mirroring `split_bracketed_stream`'s `substream_length`),
`copy_parent_paths: dict[str, str] = {}` (alias → path in the *parent* document, copied onto every
emitted item's attributes — the common "keep the header fields with each row" need).

Non-list/non-object input is forwarded unchanged. `split_json` + `concat_json_substream` is then the
canonical map-over-a-collection idiom.

*As built:* `copy_parent_paths` resolves against the document **as received**, not against whatever
`traversal_path` narrowed it to — otherwise "parent" would mean nothing, and the common case
(`traversal_path = "rows"`, copy a header field from the root) would silently copy nothing.

---

### P0-7 `ip/group_into_substreams` — id `7553a3cb-8804-49d6-9dd6-f13759055534` — **implemented**

*Category* `ip` · *Name* "Group IPs into substreams"

`wrap_into_substream` groups by *count* or an external bracket channel; this groups by *data*.
Ports: `conf`, `in`; out `out`. Config: `selector: Selector` (S1, D10),
`mode: Literal["on_key_change","buffer_all"] = "on_key_change"` (`on_key_change` assumes the stream
is already ordered by key and closes the substream as soon as the key changes — O(1) memory;
`buffer_all` buffers the entire stream and emits one substream per distinct key, in first-seen
order), `key_attr: str | None = "group_key"` (attach the key to the open-bracket IP),
`max_group_size: int = 0` (0 = unlimited; otherwise split oversized groups),
`emit_empty: bool = False`.

Pairs naturally with `sort_ips` upstream (`sort_ips` → `group_into_substreams(on_key_change)`), which
keeps memory bounded.

*As built:* incoming bracket IPs are dropped rather than nested inside the new grouping, since
keeping both would nest unpredictably. `emit_empty` was dropped: a group only exists because an IP
created it, so there is no empty group to emit.

`key_attr` is disabled with an **empty string**, not `null` — see the note under §7.9.

---

### P0-8 `ip/reduce_substream` — id `e1f2e15f-dd1f-4d3f-8d06-dd675e6a1abe` — **implemented**

*Category* `ip` · *Name* "Reduce substream"

The generic form of `concat_json_substream`, working on attributes and `common.Value` content rather
than only JSON. Ports: `conf`, `in`; out `out` (one IP per substream). Config:
`aggregations: list[Aggregation]` where each entry is
`{ selector: Selector, op: "count"|"sum"|"mean"|"min"|"max"|"first"|"last"|"list"|"set"|"concat_text", to_attr: str | None, to_content: bool = false }`,
`nesting_level: int = 0` (which bracket depth to reduce at; 0 = outermost),
`keep_open_bracket_attrs: bool = True`,
`pass_through_ips: bool = False` (if true, forward the original IPs as well and emit the reduction on
the close bracket — a "running total" mode).

Output content is a `common.Value` when a single aggregation targets the content, or a JSON object
when several do (`to_content` on more than one aggregation ⇒ JSON object keyed by `to_attr` name).

*As built:* uses S2's `collect_substream`, so nesting is handled by the helper rather than by hand,
and a substream truncated by a closing input is still reduced rather than lost. Numeric operators
skip values that are not numbers (numeric *text* counts), returning `None` when nothing numeric
remains, so one malformed IP does not void the aggregate.

---

### P0-9 `ip/flatten_substreams` — id `d4d0606e-211a-4c33-b326-134ff160a60b` — **implemented**

*Category* `ip` · *Name* "Flatten substreams"

Inverse of `wrap_into_substream`; trivial but currently missing (`split_bracketed_stream` diverts
brackets to a second port, which is not the same thing). Ports: `conf`, `in`; out `out`.
Config: `levels: int = 1` (how many nesting levels of brackets to strip; `0` = all),
`from_depth: int = 0` (strip starting at this depth, so you can flatten inner groups while keeping
the outer one), `merge_bracket_attrs: bool = True` (copy attributes that sat on the removed open
bracket onto each contained IP — otherwise they are lost).

*As built:* inner bracket attributes win over outer ones, and an IP's own attributes win over both.
Unbalanced close-brackets are forwarded with a warning rather than dropped.

---

### P0-10 `string/format_string` — id `7b8828a2-936e-4301-a907-0d6c2c73a558`

*Category* `string` · *Name* "Format string"

Generalises the `{@attr}` / `{count}` pattern already implemented inside `write_file.filename_pattern`,
which every other component currently has to do without. Ports: `conf`, `in`; out `out` (`Text`).
Config: `pattern: str` (placeholders `{@attr}`, `{@attr/sub/path}`, `{.}` for the content, `{./a/b}`
for a content path, `{count}` for the IP index, `{now:%Y-%m-%d}` for a timestamp),
`to_attr: str | None = None` (write the result into an attribute instead of the content),
`missing: Literal["error","empty","keep"] = "empty"`,
`number_format: str | None = None`.

Factor the implementation so `write_file` can be refactored onto the same helper afterwards.

---

### P0-11 `ip/probe` — id `fc669b60-ae89-46b5-9cee-044f95b01c3b` — **implemented**

*Category* `ip` · *Name* "Probe"

A pass-through logger. `console_output` is a *sink*; there is currently no way to look at a stream
without breaking it, which makes debugging any non-trivial flow painful. Ports: `conf`, `in`;
out `out` (unconnected `out` degrades to sink behaviour). Config:
`label: str = ""`, `level: Literal["debug","info","warning"] = "info"`,
`show: list[Literal["content","attributes","type","content_type","count"]] = ["count","type","content"]`,
`max_content_chars: int = 500`, `every_nth: int = 1`, `first_n: int = 0` (0 = unlimited),
`as_json: bool = False` (render the content via S3's `json_from_capnp` when a schema is resolvable),
`emit_summary_on_close: bool = True` (log total IP/bracket counts when the input closes),
`include_brackets: bool = True`, `content_type: str | None` (type to assume for untagged IPs).

*As built:* observations go to the ordinary logger, since the `log` port and `LogMessage` of §6.2
arrive with WP-1. Once they exist, probe writes them as `LogMessage` IPs on its own `role = log`
port, which makes it "tee this data stream into the log stream" rather than a special case, and the
`to: Literal["log_port","logger","both"]` field from this card is added then. It does not overlap
with the runtime `log` port: probe reports *data*, the runtime port carries *component-internal*
messages. Content with no resolvable type is reported as `<unreadable content, type unset>` rather
than guessed (D14).

Cheap to build, and the thing you will reach for most often.

---

### P0-12 `simple/sequence` — id `feec45a3-4908-4dfe-b6ab-a56be97d8f77` — **implemented**

*Category* `simple` · *Name* "Sequence"

A bounded generator; `counter` is unbounded and integer-only. Ports: `conf`, optional `trigger`
(emit the next element, or the whole sequence, per received IP); out `out`. Config:
`mode: Literal["range","list","dates"] = "range"`,
`start/stop/step` (numeric, `stop` exclusive), `values: list[ConfigValue] = []`,
`date_start/date_stop/date_step` + `date_format: str = "%Y-%m-%d"`,
`repeat: int = 1`, `wrap_in_substream: bool = False`,
`as_type: Literal["value","json","text"] = "value"`,
`index_attr: str | None = None`,
`emit: Literal["all_at_once","on_trigger","one_per_trigger"] = "all_at_once"`.

*As built:* `values` is named `sequence_values` (`values` collides with the `values` helper module
in readers' minds and reads badly next to it), `date_step` is `date_step_days: int`, and an
`integers: bool` flag controls whether a whole-number range emits ints. A trigger-paced `emit` with
no `trigger` connected falls back to emitting once, rather than stalling the flow silently.

This is the component that makes a flow self-starting and testable without a file on disk.

---

## 4. P1 — completes the symmetry (do after P0)

| Component | id | Why |
|---|---|---|
| `ip/join_ips_by_key` | `d027a5fa-d61c-483d-874b-2f3a399410c4` | Correlate IPs arriving on 2..n ports by a key selector; emit one combined IP (content from the primary port, others into attributes or a JSON object). Essential once anything runs in parallel and returns out of order — today the only synchronisation primitive is positional `ZIP`, which silently mispairs. Config needs `timeout`/`max_pending` and an `unmatched` out port. |
| `ip/gate` | `40656b19-8ee0-4f5a-b77f-2df759d89601` | Hold IPs until an `open` signal; `copy_ip_on_trigger` copies but cannot buffer-and-release. Modes: `pass_n_per_signal`, `open_close`, `drop_while_closed`. |
| `ip/take_drop_ips` | `48315511-87cb-4e86-a886-0ea28da2c500` | One component, `mode: first_n / skip_n / every_nth / last_n / while_predicate / until_predicate`, substream-scoped or stream-scoped. Test flows need this constantly. |
| `ip/deduplicate_ips` | `34ee2532-85a7-41c3-8918-d293487592a5` | By selector or content hash; `scope: stream / substream`, `window: int = 0`, optional `dup` out port. |
| `ip/content_to_attributes` | `ae18965d-c034-4836-ae44-5becd7f602a0` | Inverse of `attribute_to_content` on the extraction side: `paths: dict[str, str]` (attr name → selector), `keep_content: bool = True`, `value_type: auto/value/json/text`. Completes axis C. |
| `ip/attributes_to_content` | `cf980fe1-c991-4e41-9ae8-55b81ea20f92` | All or selected attributes → one JSON object as content (`attribute_to_content` handles exactly one attribute). Inverse mode `json_to_attributes` explodes a JSON object into attributes — put both in this component under a `direction` config. |
| `ip/map_attributes` | `7e133b74-8024-46e9-8d40-4b8333d8b9a4` | rename / keep-list / drop-list / set-default / retype in one pass. Supersedes `remove_attributes` (keep that one as a thin alias for compatibility). |
| `json/merge_json` | `807c3185-83fc-4a4f-8a4e-38b58970b711` | Deep-merge JSON from an array in-port or from `in` + `patch`; `strategy: deep/shallow/replace`, `list_strategy: replace/append/by_key`, `null_deletes: bool`. `update_json` patches from config/attrs, not from a second stream. |
| `ip/on_stream_end` | `e9fde9d5-249c-4329-b651-6ef82b79a176` | Emit a configured IP when `in` closes (or on each close bracket). The sequencing primitive for "now that everything is written, do X". |
| `ip/discard` | `af8fe95a-8040-441a-b4b3-13ee33ddbbcf` | Drain a port silently. Needed because leaving an out-port unconnected and leaving it connected-to-nothing behave differently. |

## 5. P2 — valuable, not structural

| Component | id | Note |
|---|---|---|
| `json/sort_json` | `96196bb3-1870-43f7-9a86-5aa3905fa378` | Sort an array by one or more keys; `sort_ips` sorts IPs, not array elements. |
| `json/aggregate_json` | `3d00d399-4529-45c4-86d5-9794ed0626a1` | group-by + sum/mean/min/max/count over an array — the in-document twin of `reduce_substream`. |
| `json/validate_json` | `e0212d69-b9b5-435d-8a46-d51e504a6f56` | JSON Schema or a named pydantic model → `out` / `invalid` ports. Cheap way to fail fast at flow boundaries. |
| `json/json_to_csv` | `78556789-c142-4b16-88f4-13cb855a16c4` | Generic version of `climate/timeseries_data_to_csv`. |
| `file/csv_to_json` | `49117d7d-73a6-40b8-90de-5855a8742332` | `read_csv` emits raw text; nothing parses it. Config: delimiter, header row, type inference, per-row vs. whole-table emission. |
| `string/regex` | `5a169526-70f8-4927-bcae-7729822a8405` | match / extract (named groups → attributes) / replace / split, with a `nomatch` port. |
| `string/join_strings` | `e6ebf817-4278-4ab7-a8f8-2ba398d5aca5` | Inverse of `split_string2`, over a substream. |
| `file/list_files` | `7abf9f5e-5bcf-444e-bcb9-e4237d2e40c8` | Glob → stream of paths (optionally as a substream, optionally watching). The missing generic source. |
| `ip/repeat_ip` | `af0631ec-c340-4339-b307-a3920db9a080` | Emit each input IP n times (fan-out in time, vs. `copy_ip`'s fan-out in space). |
| `convert/set_content_type` | `0585f5d5-8eb4-4704-b869-e2ed50ae8089` | Set/override `sysAttributes.contentType`; wrap/unwrap `common.StructuredText`. Small, but it is the escape hatch when an upstream component tags content wrongly. |
| `ip/stream_stats` | `26704374-e820-46f7-be07-76ec77ac8121` | Pass-through counter emitting rate/count/latency IPs on a side port every n IPs or t seconds. |
| `simple/timer` | `0085829b-1d6c-4377-85cf-cbe430c10851` | Heartbeat IP every n seconds; drives polling flows. |
| `convert/bytes_text` | `7ff3bb99-e44e-43e3-befa-1683380389b1` | base64/hex/utf-8 encode/decode; pairs with the existing chunked-IO support. |

---

## 6. The port model: roles, runtime-owned ports, and logging

Settled and **implemented** 2026-09-29 (WP-1). These three decisions changed `fbp.capnp`, the
`Process` runtime, and every component's metadata. They are grouped here because
they are one idea: *distinguish the ports that are part of a component's contract from the ports
that are part of the runtime's contract.*

### 6.1 `conf` becomes runtime-owned and continuous

**What it is today.** For `process` components the flow runner sets config via the `setConfigEntry`
RPC before `start()`, and explicitly *skips* that when the flow wires the `conf` port
(`run_fbp_flow.py`, `config_is_connected`). For legacy `standard` components an IIP on `conf` is the
only configuration path, and the runner auto-generates it. So `conf` is a redundant second write path
into state that already has a first-class RPC — which is why flows use the editor's config instead.

`Process.update_config_from_port("conf")` is a *single blocking read at the top of `run()`*. An
unconnected port is harmless (`read_in_raw` returns `None` for a `None` port), but the port is not
dynamic reconfiguration: it is deferred initial config, once, and then dead.

**Decision.** Keep the capability, drop the boilerplate:

1. The runtime owns `conf`. It is no longer declared in any component's `METADATA` and is no longer
   read in any `run()` body. `ProcessBootstrap` injects it, and `inPorts()` / the editor cache report
   it with `role = config`.
2. A background reader task applies incoming config through `apply_config_values`, which already
   revalidates against the pydantic model and is safe to call at any time.
3. **Updates are staged and applied at IP boundaries** — immediately before `read_in` returns the
   next IP, never mid-processing. This makes "config changes take effect between IPs" a rule that can
   be written down and tested, and removes torn-read hazards for components that snapshot config
   before their loop (`to_string` resolves its schema once; `json_to_common_value` computes its field
   set once — both stay correct under this rule).
4. The one thing this preserves that `setConfigEntry` cannot do: **config computed by an upstream
   component**, e.g. a flow that reads a settings file and configures its own downstream nodes.

**As built.** `ComponentMetadata` injects the ports, so they are still in the components cache the
flow editor reads and on the `inPorts`/`outPorts` RPC — a flow can wire `conf` or `log` exactly as
before, and the editor needs no change beyond optionally rendering them by `role`. What changed is
only that the *component source* no longer declares them.

`runtime/config_watcher.py` owns the port. Staged config is applied at **both** IO boundaries —
immediately before `read_in` hands an IP over, and once `write_out` has sent one. Components with no
data in-port reach neither, so `Process.next_config()` lets a source drive itself from its config::

    while True:
        emit_file(self.config.file)
        if not await self.next_config():
            break

It returns False when `conf` is unconnected or has closed, so such a loop always terminates. This is
what makes the "`read_file` reading successive files" case work; the first cut applied config only at
read boundaries and a source silently never saw an update. 33 components were stripped of their `conf`
declaration and `update_config_from_port` call; the 21 legacy `standard` ones keep theirs, since
nothing owns their port. `update_config_from_port` survives as a no-op that logs once.

One behaviour note: while waiting for an initial config that never arrives, the watcher now says so
every 15 seconds. It still waits indefinitely - timing out and running with defaults would be worse
than a visibly stalled flow.

### 6.2 A runtime-owned `log` out-port

**Decision: adopt it.** Two reasons, the second being the decisive one:

- Logs become ordinary IPs, so the base components *are* the log pipeline:
  `filter_ips → format_string → write_file` is a log router obtained for free, and every improvement
  to the algebra improves logging.
- **Cross-language uniformity.** Making a Python `logging` handler, a Go logger and a C++ logger
  agree on routing is a permanent maintenance tax; making them agree on "write a Cap'n Proto struct
  to a channel" is what they already all do. The runtime can wire every component's `log` port and
  forward wherever needed, without hooking into any language's logging framework.

**Four non-negotiable constraints.** These are where a naive implementation goes wrong:

1. **Lossy, non-blocking writes.** Channel `write` blocks when the buffer is full, so a component
   logging inside its loop can block *on logging* if the consumer stalls — and a consumer downstream
   in the same flow can form a deadlock cycle. Log writes must use `Channel.Writer.writeIfSpace`
   (already in `fbp.capnp`) and drop-with-a-counter on failure. Nothing in the Python runtime uses
   `writeIfSpace` yet, so this needs an `OutputRuntime.write_out_if_space` addition.
   *Logging must never be able to stall or deadlock a flow.*
2. **The port is a tee, never a replacement.** The local logger stays on always, with two independent
   levels: `log_level` for stderr and `log_port_level` for the port (typically more verbose).
   Otherwise everything emitted before the port is connected or after it closes is lost — precisely
   when it is most needed.
3. **Never log the log path.** Failures writing to the `log` port go to the local logger only.
4. **Bound the payload.** Truncate message text and traceback frames. A node with
   `parallel_count > 1` puts N writers on one log channel (supported), so the record needs
   `processId` to disambiguate, and a hot `DEBUG` loop at N-way parallelism will otherwise flood it.

**As built.** Records go onto a bounded deque (so `emit()` can never block the component that
logged) and are drained by a task that writes them with `OutputRuntime.write_out_if_space`. A
channel predating `writeIfSpace` reports it unimplemented; dropping is the right answer there too,
since this path exists so it can never block. Drop counts are reported once when the process closes.
`probe` needs no log port of its own after all: it logs normally, and the tee turns its observations
into `LogMessage` IPs whenever a flow connects `log`.

**Record type.** Added to `fbp.capnp`, deliberately aligned with the existing
`Process.RunInfo` vocabulary so "something went wrong" reads the same whether it arrives via
`lastRun` or via the port:

```capnp
struct LogMessage {
  enum Level { debug @0; info @1; warning @2; error @3; critical @4; }
  level       @0 :Level;
  timestamp   @1 :Text;      # ISO 8601
  processId   @2 :Text;
  processName @3 :Text;
  logger      @4 :Text;
  message     @5 :Text;
  attributes  @6 :List(IP.KV);   # structured fields
  traceback   @7 :List(Text);    # same convention as Process.RunInfo
}
```

**The alternative rejected on purpose.** `Process` already has `state(transitionCallback)` and
`activity(transitionCallback)` — push-based RPC observability. A `logs(callback, minLevel)` method
would be the *consistent* choice with what is already there, keeps non-dataflow concerns out of the
dataflow graph, and carries no backpressure risk. It is a respectable option and was considered. The
port wins because the runtime wiring all `log` ports itself recovers the uniform tooling path anyway,
while the callback route can neither be drawn in the flow editor nor composed with the library's own
components. The objection it embodies — "logs are not dataflow, so keep them out of the dataflow
graph" — is answered by §6.3: `role` keeps them out of the *contract* even though they travel on the
same transport.

### 6.3 Mark ports with metadata fields, not name sigils

**Decision: extend `Component.Port`; do not use `_`/`*` prefixes.**

Against sigils:

1. They encode meaning in a string that every language runtime must parse identically, and
   immediately raise "does the wire name include the sigil?" (`connectInPort(name="_conf")` vs
   `"conf"`). That is exactly the class of cross-language bug the `log` port exists to avoid.
2. There is already a typed port model — `ComponentPortMetadata` (`run/metadata.py`) and
   `Component.Port` (`fbp.capnp`), which already carries `type :PortType` for standard/array. A
   second enum extends a pattern already committed to, is validated, and flows into
   `local_components_cache.json` for the editor at no extra cost.
3. Sigils do not survive the editor. `_conf` vs `conf` is a weak visual signal; a field lets the flow
   tool *render* config and log ports differently — dimmed, on another edge of the node, or folded
   behind a toggle — which is the point of marking them.
4. `_` already means "private" in Python; `*` is awkward in JSON keys, URLs and shells, and reads as
   a glob.

**The structural point:** `conf`/`log` and `err`/`rej` differ along *orthogonal* axes, and one sigil
can only express one of them.

| Axis | Values | `conf` / `log` | `err` / `rej` / `pass` / `brackets` / `trigger` | `in` / `out` |
|---|---|---|---|---|
| **Ownership** | runtime / component | runtime | component | component |
| **Necessity** | required / optional | optional | optional | usually required |
| **Kind** | data / config / log / error / reject / control | config, log | error, reject, control | data |

Each axis earns its keep: **ownership** tells the editor a port is not part of the component's
contract and tells the runtime it may auto-wire it; **necessity** enables the validation "this flow
leaves a required port unconnected", which nothing can express today; **kind** gives the generic
tooling payoff — *a runtime that auto-wires every `error`-kind port to a dead-letter collector is the
same feature as one that auto-wires every `log` port.* That is the reason to mark `err`/`rej` after
all: not as always-present default ports, but as a kind tooling can reason about generically.

In `fbp.capnp`, on `Component.Port`:

```capnp
enum PortRole { data @0; config @1; log @2; error @3; reject @4; control @5; }
role     @4 :PortRole = data;
required @5 :Bool = false;
```

Mirrored on `ComponentPortMetadata` as `role: Literal[...] = "data"` and `required: bool = False`.

**Reserved names.** `conf`, `log`, `err`, `rej` are reserved. A `model_validator` on
`ComponentMetadata` rejects a port named `conf` or `log` whose role does not match, and rejects a
`config`/`log` role on any other name. Cheap, and it catches drift before it propagates across three
language implementations.

**Shipped.** `fbp.capnp` lives in the separate `mas_capnproto_schemas` repository (shipped here as
the `zalfmas-capnp-schemas` dependency), so 6.2 and 6.3 were a cross-repo change, bundled with the
array-in-port comment fix (D5) so the schema was touched once. Released as 0.1.70.

Backwards compatible, verified in both directions against the released 0.1.69 package: a `Port`
written by the old schema reads back with `role = data` and `required = false`, and one written by
the new schema still reads correctly as the old type. Ordinals 4 and 5 were free on `Port` and both
fields fit in its existing data word, so an encoded `Port` is the same 72 bytes as before.

---

## 7. Decision register

All cross-cutting decisions are settled as of 2026-09-29. Recorded here so implementers do not
re-open them, and so the reasoning survives.

| # | Decision | Settled |
|---|---|---|
| D1 | Selector syntax: sigils mark selectors | §7.1 |
| D2 | Error/reject ports: `err` (`role = error`) / `rej` (`role = reject`) | §6.3 |
| D3 | Bracket transparency is the library default | §7.2 |
| D4 | Attribute values: write `common.Value`, read both | §7.3 |
| D5 | Array **in**-ports are officially supported | §7.4 |
| D6 | Conversions live in a new `convert` category | §7.5 |
| D7 | Per-index array write is `Process.write_array_out_at` | §7.5 |
| D8 | Comparisons coerce permissively by default | §7.6 |
| D9 | Shared modules live in `zalfmas_fbp/components/common/` | §7.6 |
| D10 | Config field naming: `selector`, and `left`/`right` in predicates | §7.6 |
| D11 | Digit path segments are list indices | §7.6 |
| D12 | S3 refactor of `to_string` / `json_to_common_value` happens in WP0, tests first | §7.7 |
| D13 | WP-1 and WP0 proceed in parallel | §8 |
| D14 | Reads are type-driven; no struct-cast guessing, `MISSING` instead | §2 (S3) |
| D15 | List and scalar integer field selection unified on unsigned-first | §7.8 |

### 7.1 D1 — sigils mark selectors

Only strings beginning with a sigil are selectors; everything else in a predicate's `right` (or any
literal position) is a literal value:

| Form | Meaning |
|---|---|
| `@attr`, `@attr/sub/0` | attribute, optionally with a path into its value |
| `.`, `./a/b/0` | the IP content, optionally with a path into it |
| `#type`, `#contentType` | IP metadata |
| `gjson:<query>` | opt-in GJSON query against JSON content |
| anything else | literal (bare numbers, `true`/`false`/`null` are typed literals) |

A literal string that genuinely starts with a sigil is escaped with a backslash: `"\@zalf.de"` is the
literal `@zalf.de`. `parse_selector` must therefore unescape a leading `\` and this must be covered
by a unit test. Chosen for brevity and because it is already the de-facto convention in
`update_json` (`@attr/sub`) and `write_file.filename_pattern` (`{@attr}`).

### 7.2 D3 — bracket transparency is the default

Any component that does not explicitly reason about substreams forwards bracket IPs unchanged. This
is a library invariant, not a per-component choice: enforce it through the S2 helper
(`BracketPolicy.TRANSPARENT`) and document it in `agents_process.md` §4.1.

### 7.3 D4 — attribute values: write `common.Value`, read both

Base components **always write** attribute values as `common.Value` with `valueType` set. The S1
resolver **accepts both** `common.Value` and a raw Text/AnyPointer value when reading, so existing
flows and components that assign plain `str` keep working. The tolerant branch lives in exactly one
place (`values.py:python_from_attr`), not scattered across components.

This resolves the open question in `agents_process.md` §4.3 — update that file to state the rule.

### 7.4 D5 — array in-ports are supported

Confirmed: array in-ports are a supported feature. `InputRuntime.read_array_in` implements them and
`dakis/merge_geoparquet` already uses one. The `fbp.capnp` comment on `Component.Port.PortType`
still reads `array @1; # array port (only an out port can be an array port)` — that restriction is
obsolete and the comment must be corrected in the WP-1 schema change.

### 7.5 D6, D7 — categories and the array-write API

Conversion components go in a new `convert` category rather than growing `json`, since they are
equally about Cap'n Proto; this keeps the palette legible in the flow editor.

`OutputRuntime.write_array_out_port(name, index, port, message)` already does the per-index write
P0-2 needs but is not exposed on `Process`. Add `Process.write_array_out_at(name, index, message)`,
resolving the writer from `array_out_ports[name][index]` and returning `False` for a disconnected
slot.

### 7.6 D8–D11 — S1/S3 conventions

- **D8, permissive coercion.** Comparisons coerce by default: a TOML `right = "2020"` compares equal
  to a capnp `ui64` 2020. Config files cannot express capnp's numeric types, so strict-by-default
  would turn every predicate into a type-matching puzzle. Opt out per component with
  `strict_types: bool = False`.
- **D9, module home.** `zalfmas_fbp/components/common/` — these are component-authoring support, not
  runtime. Precedent: `components/dakis/common/`.
- **D10, config field naming.** A single-value selector field is named `selector`; predicates use
  `left` / `op` / `right`. Normalise the P0 cards accordingly (P0-7 says `key`, P0-8 says `source`).
- **D11, digit path segments** are list indices, matching `json_to_common_value._split_path` today,
  so `./items/0` indexes rather than looking up the key `"0"`.

### 7.8 D15 — list and scalar integer field selection unified

The D12 refactor surfaced an inconsistency in `json/json_to_common_value`: scalars tried **unsigned**
integer fields first, lists tried **signed** first, so the same number picked a different width
depending on where it sat — `200` was `ui8` but `[200]` was `li16`, and `[1, 2, 3]` was `li8`. Lists
therefore spent an extra byte per element for values in 128–255, and a consumer switching on
`which()` saw different types for the same data.

**Decision: unify on the scalar (unsigned-first) rule.** `200` and `[200]` now both select
`ui8`/`lui8`. Negative values still select signed fields, as before.

This is wire-visible: a non-Python consumer switching on the Cap'n Proto union field will see
`lui8`/`lui16` where it previously saw `li8`/`li16`. Python consumers are unaffected —
`python_from_value` yields the same integers either way.

**A latent bug this exposed, and the rule that resolves it.** The sentinel attribute that
`json_to_common_value` attaches was typed to match the payload's selected field, so a *negative*
`null_sentinel` configured for an all-positive payload could not be represented — the build raised,
and with `skip_on_error` the whole message was silently dropped. This already happened for scalars
before D15 (`42` with `null_sentinel = -1` produced nothing); unifying merely widened it to lists.

The real problem was that type selection ignored the sentinel unless a `null` happened to put it in
the data. That also made the emitted type depend on the payload: a stream would emit `lui8` for
messages containing no nulls and `li16` for the ones that did, so a consumer switching on the union
field saw the type flap message to message.

**Rule: a configured sentinel belongs to the value domain whether or not it currently appears, and
the chosen type must accommodate it.** `values.value_from_python(..., must_accommodate=[...])`
implements this, applying at every leaf since a null could appear anywhere. Accommodated values take
part in selection exactly as if they were elements, so the two cases are now identical:

| Payload | Sentinel | Field |
|---|---|---|
| `[1, 2]` | `-1` | `li8` — same as `[1, -1]` |
| `[1, None, 2]` | `-9999` | `li16` — same as `[1, 2]` with that sentinel |
| `[1, 2]` | `999` | `lui16` — a positive sentinel does not force a signed type |
| `[1, 2]` | `"N/A"` | `lv` — same as `[1, "N/A"]` |

One case cannot be accommodated: a scalar payload of one kind with a sentinel of another (text
content with a numeric sentinel), where no single `Value` field holds both. There the sentinel
attribute falls back to its own type — the attribute is worth more than the type match, and dropping
the message was never the intent.

### 7.9 A config `null` means "use the default", not "set to null"

Found while building WP4, and worth knowing before writing any component with a nullable field.
`ProcessConfigRuntime.apply_config_values` treats a `None` as *remove this key*:

```python
if value is None:
    next_raw_config.pop(key, None)
    continue
```

So a field with a non-null default cannot be turned off from a flow config — sending
`key_attr = null` restores `"group_key"` rather than unsetting it. This is sensible for
`setConfigEntry` (where removing an entry is a real operation), but it means a component wanting an
"off" setting must accept one that is expressible.

**Convention:** for optional *name* fields with a non-null default, treat the **empty string** as
off, and say so in the field description. `group_into_substreams.key_attr` and
`split_json.count_attr` both do.

### 7.7 D12 — refactor the two existing users onto S3 in WP0

`to_string` and `json_to_common_value` move onto `values.py` as part of WP0, **characterization
tests first**: capture current behaviour, then refactor, then show the tests still pass. The point is
to validate the abstraction before twelve components depend on it — if S3 cannot absorb its two
existing users, it is the wrong shape and better to find out in WP0 than in WP4.

**Outcome (done).** 33 characterization tests were written for `json_to_common_value`, which had
none, plus 2 for `to_string`'s fallback paths. All passed unmodified after the refactor. The
component shed 399 lines; `to_string` lost its private schema-resolution helper. The abstraction
held, with one genuine behavioural difference found and preserved rather than absorbed — see D15,
which is exactly the kind of thing characterization tests exist to catch.

---

## 8. Suggested work packages

Each package is independently mergeable and leaves the library in a working state.

| WP | Contents | Rough size |
|---|---|---|
| **WP-1** ✅ | The port model (§6). `fbp.capnp`: `PortRole` + `required` on `Component.Port`, `LogMessage`, array-in-port comment fix. Runtime: inject runtime-owned `conf`/`log` ports, staged config application at IP boundaries, `OutputRuntime.write_out_if_space`, `log_port_level`, the log-record tee. Metadata: `role`/`required` fields + reserved-name validator. Strip `conf` from existing component metadata and `run()` bodies. **Cross-repo** (`mas_capnproto_schemas`). Done; the schema change shipped as 0.1.70 and is
backwards compatible in both directions at no wire cost. | 1 large change, 2 repos |
| **WP0** ✅ | S1 selectors, S2 brackets, S3 values + unit tests. No components. Then characterization tests for `to_string` and `json_to_common_value`, and refactor both onto S3 (D12) as the proof that the abstractions fit. Independent of WP-1, so the two run in parallel (D13). | 1 sizeable change |
| **WP1** ✅ | P0-11 `probe`, P0-12 `sequence`, P0-9 `flatten_substreams`. Done; 49 tests. Written against the *current* `conf` convention since WP-1 has not landed — each needs the same mechanical retrofit afterwards (drop the `conf` port from metadata, drop the `update_config_from_port` line). `probe` logs to the ordinary logger until the `log` port of §6.2 exists. | 3 small components |
| **WP2** ✅ | P0-4 `capnp_to_json`, P0-5 `json_to_capnp`. The representation bridge. Done; 38 tests including round-trips over `StructuredText`, `Value` and `IP` (a struct with a nested list of structs and an enum), plus a two-lap test so the conversion is stable rather than merely reversible once. | 2 medium components |
| **WP3** ✅ | P0-1 `filter_ips`, P0-2 `route_ips`, P0-3 `merge_ips` (+ `Process.write_array_out_at` and `read_array_in_with_index`). Semantic routing. Done; 34 tests. | 3 medium components |
| **WP4** ✅ | P0-6 `split_json`, P0-7 `group_into_substreams`, P0-8 `reduce_substream`. The substream algebra. Done; 61 tests, including the `split → filter → group → reduce` pipeline run end to end. | 3 medium components |
| **WP5** | P0-10 `format_string` + refactor `write_file` onto it. | 1 small component + refactor |
| **WP6** | P1 set, in the order listed (join-by-key first — it unblocks any parallel-service flow). | 10 components |
| **WP7** | P2 set, on demand. | — |

After WP4 the library covers every cell of the §1 matrix at least once, which is the point at which
"mix and match for everyday needs" becomes true.

### Definition of done, per component

1. Module under `zalfmas_fbp/components/<category>/<name>.py` following `agents_process.md` §1.
2. UUID from this document, registered in `configs/local_cmds.json`.
3. Bracket policy explicitly chosen and stated in the metadata description.
4. Every declared port carries an explicit `role` and `required` (§6.3); `conf`/`log` are *not*
   declared.
5. Unit test in `tests/components/<category>/test_<name>.py` covering: happy path, malformed input,
   missing selector/config, bracket pass-through, attribute propagation.
6. One line in `CHANGELOG.md` via the usual `feat:` commit convention.
7. A one-line entry appended to the catalogue table in this file, so §1 stays current.
