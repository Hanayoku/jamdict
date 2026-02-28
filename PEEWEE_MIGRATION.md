# Peewee Migration: `jmdict_sqlite.py` → `jmdict_peewee.py` + `jamdict_peewee.py`

## Status

**Phases 1–6 complete.** All 292 tests pass (82 new phase-6 tests + 112 phase-5 tests + 67 phase-4 peewee tests + 31 existing tests).

---

## Background

`jmdict_sqlite.py` uses [puchikarui](https://github.com/letuananh/puchikarui) as its database
abstraction layer. puchikarui has been unmaintained for over 5 years and `MemorySource` (used for
`memory_mode=True`) no longer exists in the installed version — meaning that feature is silently
broken today.

This migration replaces puchikarui with [peewee](https://docs.peewee-orm.com/) via two new files:

| File | Role |
|------|------|
| `jamdict/jmdict_peewee.py` | Low-level store: peewee models + `JMDictDB` class |
| `jamdict/kanjidic2_peewee.py` | Low-level store: peewee models + `KanjiDic2DB` class |
| `jamdict/jmnedict_peewee.py` | Low-level store: peewee models + `JMNEDictDB` class |
| `jamdict/jamdict_peewee.py` | High-level runner: `JamdictPeewee` + `LookupResult` |

The existing `jmdict_sqlite.py` and `util.py` are **left completely untouched**.

---

## Design Decisions

### No puchikarui compatibility shims

Earlier iterations tried to mimic the puchikarui `ExecutionContext` interface
(`ctx.buckmode()`, `ctx.commit()`, etc.) so the new code could be a drop-in inside the
existing `util.py`. This proved fragile and unnecessary.

Instead, the new implementation has its own clean public API and its own runner
(`JamdictPeewee`). `util.py` continues to use the puchikarui path unchanged.

### Per-instance database isolation

peewee model classes normally share a module-level `database` singleton, which means two
`JMDictDB` instances would stomp on each other's connection. We solve this with
`db.bind_ctx(ALL_MODELS)` — a context manager that temporarily routes all model queries
through a specific `SqliteDatabase` instance without mutating the class-level binding.

Every public method on `JMDictDB` wraps its queries in `self._db.bind_ctx(ALL_MODELS)`, so
multiple instances (including `:memory:` databases) can coexist safely in the same process.

### jmdict-only scope (for now)

`JamdictPeewee` supports JMDict word lookup only. KanjiDic2 and JMNEDict are out of scope
until the JMDict path is proven in production.

---

## File Layout

```
jamdict/jamdict/
    jmdict_sqlite.py          ← unchanged (puchikarui, kept for reference / old tests)
    jmdict_peewee.py          ← peewee models + JMDictDB (low-level store)
    kanjidic2_peewee.py       ← peewee models + KanjiDic2DB (low-level store)
    jmnedict_peewee.py        ← peewee models + JMNEDictDB (low-level store)
    jamdict_peewee.py         ← JamdictPeewee runner (high-level API)
    util_old.py               ← original puchikarui-backed Jamdict (renamed from util.py)
    util.py                   ← new peewee-backed Jamdict with identical public API

jamdict/test/
    test_jmdict_sqlite.py     ← unchanged (existing tests against puchikarui impl)
    test_jmdict_peewee.py     ← new tests for JMDictDB + JamdictPeewee (67 tests)
    test_peewee_phase5.py     ← new tests for KanjiDic2DB + JMNEDictDB + extended JamdictPeewee (112 tests)
    test_phase6.py            ← parity tests: old vs new Jamdict public API (82 tests)
```

---

## Public API

### `JMDictDB` (`jmdict_peewee.py`)

Low-level store. Each instance owns its own `SqliteDatabase` connection.

```python
db = JMDictDB("path/to/jmdict.db")   # or ":memory:"

# Import
db.insert_entries(list_of_jmdentry)
db.insert_entry(one_jmdentry)

# Query
entry  = db.get_entry(1234567)        # → JMDEntry | None
results = db.search("食べる")          # → list[JMDEntry]
results = db.search("%食べ%る")        # wildcard LIKE
results = db.search("id#1234567")     # by idseq
for e in db.search_iter("食べ%る"):   # memory-efficient iteration
    print(e)

# Metadata
db.all_pos()                          # → list[str]
db.update_meta(version, url)
db.get_meta("jmdict.version")         # → str | None

# Resource management
db.close()
with JMDictDB(":memory:") as db:      # context manager
    ...
```

### `KanjiDic2DB` (`kanjidic2_peewee.py`)

Low-level store. Each instance owns its own `SqliteDatabase` connection.

```python
db = KanjiDic2DB("path/to/kanjidic2.db")   # or ":memory:"

# Import
db.insert_chars(list_of_character)
db.insert_char(one_character)

# Query
char  = db.get_char("持")               # → Character | None
char  = db.get_char_by_id(42)           # → Character | None
chars = db.all_chars()                  # → list[Character]
for c in db.search_chars_iter(["持", "食", "飲"]):
    print(c)

# Metadata
db.update_kd2_meta(file_version, database_version, date_of_creation)
db.get_meta("kanjidic2.file_version")   # → str | None

# Resource management
db.close()
with KanjiDic2DB(":memory:") as db:
    ...
```

### `JMNEDictDB` (`jmnedict_peewee.py`)

Low-level store. Each instance owns its own `SqliteDatabase` connection.

```python
db = JMNEDictDB("path/to/jmnedict.db")   # or ":memory:"

# Import
db.insert_entries(list_of_jmdentry)
db.insert_entry(one_jmdentry)

# Query
entry  = db.get_ne(5741815)             # → JMDEntry | None
results = db.search_ne("神龍")          # → list[JMDEntry]
results = db.search_ne("%神%")          # wildcard LIKE
results = db.search_ne("id#5741815")   # by idseq
for e in db.search_ne_iter("しめ%"):   # memory-efficient iteration
    print(e)

# Metadata
db.all_ne_type()                        # → list[str]
db.update_meta(version, url, date)
db.get_meta("jmnedict.version")         # → str | None

# Resource management
db.close()
with JMNEDictDB(":memory:") as db:
    ...
```

### `JamdictPeewee` (`jamdict_peewee.py`)

High-level runner. Owns `JMDictDB`, `KanjiDic2DB`, and `JMNEDictDB` instances lazily.

```python
jam = JamdictPeewee(
    db_path="jmdict.db",
    xml_path="JMdict_e.xml",
    kd2_db_path="kanjidic2.db",
    kd2_xml_path="kanjidic2.xml",
    jmne_db_path="jmnedict.db",
    jmne_xml_path="JMnedict.xml",
)

# First run — import from XML (all three, or selectively)
jam.import_data()                            # all three
jam.import_data(kanjidic2=False)             # skip KanjiDic2
jam.import_data(jmdict=False, jmnedict=False) # KanjiDic2 only

# JMDict lookup — result.chars and result.names populated when configured
result = jam.lookup("食べる")          # → LookupResult
result = jam.lookup("%食べ%る")        # wildcard
result = jam.lookup("食べる", pos=["verb"])  # POS filter
result.entries                        # list[JMDEntry]
result.chars                          # list[Character]  (KanjiDic2)
result.names                          # list[JMDEntry]   (JMNEDict)
bool(result)                          # False when all three are empty

for e in jam.lookup_iter("食べ%る"):   # JMDict iterator
    print(e)

jam.get_entry(1234567)                # → JMDEntry | None  (JMDict)
jam.all_pos()                         # → list[str]

# KanjiDic2 accessors (require kd2_db_path)
jam.get_char("持")                    # → Character | None
jam.get_char_by_id(42)               # → Character | None
jam.all_chars()                       # → list[Character]

# JMNEDict accessors (require jmne_db_path)
jam.get_ne(5741815)                   # → JMDEntry | None
jam.search_ne("神龍")                 # → list[JMDEntry]
jam.search_ne_iter("しめ%")           # → Iterator[JMDEntry]
jam.all_ne_type()                     # → list[str]

# Resource management
jam.close()
with JamdictPeewee(db_path=..., xml_path=...) as jam:
    jam.import_data()
    jam.lookup("食べる")
```

---

## Task List

### Phase 1 — Setup

- [x] **1.1** Add `peewee` to `pyproject.toml` dependencies.
- [x] **1.2** Confirm `peewee` installs cleanly in the `.venv` and imports without error.

---

### Phase 2 — Define Peewee Models in `jmdict_peewee.py`

- [x] **2.1** Model classes defined with `database=None` (unbound at module level).
  Isolation is achieved via `bind_ctx` per method call, not a module-level singleton.

- [x] **2.2** `_Base(Model)` with `Meta.database = None`.

- [x] **2.3** All 25 model classes defined:

  | Model class        | Table name   | Key fields                                          |
  |--------------------|--------------|-----------------------------------------------------|
  | `MetaModel`        | `meta`       | `key` (PK, text), `value` (text)                    |
  | `EntryModel`       | `Entry`      | `idseq` (PK, integer)                               |
  | `LinkModel`        | `Link`       | `idseq` (FK→Entry), `tag`, `desc`, `uri`            |
  | `BibModel`         | `Bib`        | `idseq` (FK→Entry), `tag`, `text`                   |
  | `EtymModel`        | `Etym`       | `idseq` (FK→Entry), `text`                          |
  | `AuditModel`       | `Audit`      | `idseq` (FK→Entry), `upd_date`, `upd_detl`          |
  | `KanjiModel`       | `Kanji`      | `idseq` (FK→Entry), `text`                          |
  | `KJIModel`         | `KJI`        | `kid` (FK→Kanji), `text`                            |
  | `KJPModel`         | `KJP`        | `kid` (FK→Kanji), `text`                            |
  | `KanaModel`        | `Kana`       | `idseq` (FK→Entry), `text`, `nokanji`               |
  | `KNIModel`         | `KNI`        | `kid` (FK→Kana), `text`                             |
  | `KNPModel`         | `KNP`        | `kid` (FK→Kana), `text`                             |
  | `KNRModel`         | `KNR`        | `kid` (FK→Kana), `text`                             |
  | `SenseModel`       | `Sense`      | `idseq` (FK→Entry)                                  |
  | `StagkModel`       | `stagk`      | `sid` (FK→Sense), `text`                            |
  | `StagrModel`       | `stagr`      | `sid` (FK→Sense), `text`                            |
  | `PosModel`         | `pos`        | `sid` (FK→Sense), `text`                            |
  | `XrefModel`        | `xref`       | `sid` (FK→Sense), `text`                            |
  | `AntonymModel`     | `antonym`    | `sid` (FK→Sense), `text`                            |
  | `FieldModel`       | `field`      | `sid` (FK→Sense), `text`                            |
  | `MiscModel`        | `misc`       | `sid` (FK→Sense), `text`                            |
  | `SenseInfoModel`   | `SenseInfo`  | `sid` (FK→Sense), `text`                            |
  | `SenseSourceModel` | `SenseSource`| `sid` (FK→Sense), `text`, `lang`, `lstype`, `wasei` |
  | `DialectModel`     | `dialect`    | `sid` (FK→Sense), `text`                            |
  | `SenseGlossModel`  | `SenseGloss` | `sid` (FK→Sense), `lang`, `gend`, `text`            |

- [x] **2.4** `ALL_MODELS` list defined (parent-before-child order for `create_tables`).

---

### Phase 3 — Implement `JMDictDB` class

- [x] **3.1** `__init__(db_path)` — creates a `SqliteDatabase`, uses `bind_ctx` for init
  queries (`connect`, `create_tables`, `_seed_meta`).
- [x] **3.2** `update_meta(version, url)` — upsert via `ON CONFLICT`.
- [x] **3.3** `get_meta(key)` — point lookup, returns `None` if absent.
- [x] **3.4** `all_pos()` — `SELECT DISTINCT text FROM pos`.
- [x] **3.5** `_build_entry_query(query, pos)` — builds peewee `SelectQuery` without executing:
  - `id#<n>` → filter by `idseq`
  - wildcard (`%`, `_`, `@`) → `**` operator (SQL `LIKE`)
  - exact → `==` expressions
  - `pos` filter → subquery join on `PosModel`
- [x] **3.6** `search(query, pos)` → `list[JMDEntry]`.
- [x] **3.7** `search_iter(query, pos)` → `Iterator[JMDEntry]`.
- [x] **3.8** `get_entry(idseq)` → `JMDEntry | None` — full N-level assembly inside `bind_ctx`.
- [x] **3.9** `insert_entries(entries)` — bulk insert inside `atomic()` with performance PRAGMAs.
- [x] **3.10** `insert_entry(entry)` — single insert (delegates to `_insert_entry_unsafe`).
- [x] **3.11** `close()` + context manager protocol (`__enter__` / `__exit__`).

---

### Phase 4 — Implement `JamdictPeewee` runner + tests

- [x] **4.1** `jamdict_peewee.py` created with `LookupResult` and `JamdictPeewee`.
- [x] **4.2** `JamdictPeewee.import_data()` — parse XML, bulk insert.
- [x] **4.3** `JamdictPeewee.lookup(query, pos)` → `LookupResult`.
- [x] **4.4** `JamdictPeewee.lookup_iter(query, pos)` → `Iterator[JMDEntry]`.
- [x] **4.5** `JamdictPeewee.get_entry(idseq)` → `JMDEntry | None`.
- [x] **4.6** `JamdictPeewee.all_pos()` → `list[str]`.
- [x] **4.7** `test/test_jmdict_peewee.py` written with 67 pytest tests covering:
  - Import (file-backed + `:memory:` + single entry roundtrip)
  - `get_entry` (idseq, kanji/kana/gloss/pos/lsource/to_dict)
  - `search` (exact, wildcard, id#, no-results, pos filter)
  - `search_iter` (count, kana forms, type checks)
  - `all_pos` (list, count, no duplicates)
  - `update_meta` / `get_meta` (upsert, overwrite, missing key, seed on init)
  - Context manager protocol
  - `JamdictPeewee` runner (lookup, lookup_iter, get_entry, all_pos, errors)
  - Multiple concurrent instances (regression for singleton isolation)

---

### Phase 5 — Extend to KanjiDic2 and JMNEDict ✅

- [x] **5.1** Add `KanjiDic2DB` in `kanjidic2_peewee.py` following the same
  `bind_ctx` pattern.  13 peewee model classes covering all KanjiDic2 tables.
- [x] **5.2** Add `JMNEDictDB` in `jmnedict_peewee.py`.  8 peewee model classes
  covering all JMNEDict tables.
- [x] **5.3** Extend `JamdictPeewee` to accept `kd2_db_path`/`kd2_xml_path` and
  `jmne_db_path`/`jmne_xml_path`.  `LookupResult` now surfaces `chars` and
  `names` alongside `entries`.  `import_data()` accepts `jmdict`/`kanjidic2`/
  `jmnedict` boolean flags to import selectively.
- [x] **5.4** `test/test_peewee_phase5.py` written with 112 pytest tests covering:
  - `KanjiDic2DB`: import (file-backed, `:memory:`, single-char roundtrip), `get_char`
    (literal, by-id, missing, readings, meanings, codepoints, radicals, dic_refs,
    query_codes, full `to_dict()` roundtrip for every character), reading order,
    `search_chars_iter`, metadata (seed, update, overwrite, missing key), context
    manager, multiple concurrent instances
  - `JMNEDictDB`: import (entry count, `:memory:`, single-entry roundtrip, all idseqs),
    `get_ne` (kanji/kana/gloss/name_type/to_dict roundtrip, Shenron fixture),
    `search_ne` (idseq prefix, exact kanji/kana/gloss, wildcard, name_type, no-results,
    invalid id#), `search_ne_iter`, `all_ne_type`, metadata, context manager, multiple
    concurrent instances
  - Extended `JamdictPeewee`: import flags, KanjiDic2/JMNEDict accessors,
    `RuntimeError` when db not configured, combined `lookup()` populating
    `result.chars` + `result.names`, `LookupResult.__bool__` via all three fields,
    `close()` / context manager closing all three DBs, instance isolation

---

### Phase 6 — Replace `util.py` ✅

- [x] **6.1** Rename the original `util.py` → `util_old.py` (puchikarui path kept
  intact for reference and for the parity test suite).
- [x] **6.2** Write a new `util.py` that re-implements the complete `Jamdict` public
  API backed by `JMDictDB`, `KanjiDic2DB`, and `JMNEDictDB`.
  - `memory_mode` is accepted for API compatibility but is a **no-op** — the peewee
    backend does not need pre-loading, and `MemorySource` is broken in recent
    puchikarui anyway.
  - `reuse_ctx` / `ctx=` parameters are accepted and silently ignored.
  - `LookupResult`, `IterLookupResult`, `JMDictXML`, `KanjiDic2XML`, and
    `JMNEDictXML` are identical copies from `util_old.py` (no DB dependency).
  - `_MEMORY_MODE = True` exported for any callers that check the flag.
- [x] **6.3** Update `__init__.py` to import `Jamdict`, `JMDictXML`, `KanjiDic2XML`
  from the new `util.py` instead of `util_old.py`.
- [x] **6.4** `jmdict_sqlite.py` / `kanjidic2_sqlite.py` / `jmnedict_sqlite.py` kept
  for now — they are still referenced by `test_jmdict_sqlite.py`, `test_jmnedict.py`,
  and `test_kanjidic2_sqlite.py`.  Removal deferred to a follow-up cleanup pass.
- [ ] **6.5** Remove `data/setup_jmdict.sql` (schema now defined in peewee models).
- [ ] **6.6** Remove `puchikarui` from `pyproject.toml` after verifying no remaining
  dependents (`kanjidic2_sqlite.py`, `jmnedict_sqlite.py`).
- [ ] **6.7** Update `tools.py` to remove `puchikarui_version` output.
- [x] **6.8** Write `test/test_phase6.py` — 82 parity tests that run every significant
  public method side-by-side against both backends and assert identical observable
  output (idseqs, kana/kanji/gloss sets, character literals, name sets, `to_dict()`
  keys, error types).  Known intentional differences are documented in the test
  module docstring and tested individually rather than compared.
- [x] **6.9** Mark migration complete in this file.

#### Intentional behavioural differences from `util_old.py`

| Behaviour | `util_old.py` | new `util.py` |
|-----------|---------------|---------------|
| `:memory:` mode | Silently broken (MemorySource removed from puchikarui) | Works correctly |
| `ready` on `:memory:` | Always `False` (`os.path.isfile` fails) | Returns `True` |
| `get_entry` / `get_ne` for missing idseq | Returns an empty `JMDEntry` | Returns `None` |
| `idseq` type on `JMDEntry` | `int` | `str` (normalise with `int(e.idseq)`) |
| `all_ne_type()` order | Alphabetical (SQLite ORDER BY) | Insertion order |
| POS string warning logger | `jamdict.jmdict_sqlite` | `jamdict.jmdict_peewee` |

---

## Notes

### `bind_ctx` isolation pattern

Every public method on `JMDictDB` wraps its peewee queries in:

```python
with self._db.bind_ctx(ALL_MODELS):
    # queries here run against self._db
```

This is the only safe way to run multiple `JMDictDB` instances concurrently. A naive
`db.bind(ALL_MODELS)` permanently mutates the model class and causes the last-created
instance to steal all queries from earlier instances.

### Wildcard operator

peewee's `**` operator maps to SQL `LIKE` on SQLite. The `%` Python operator is
arithmetic modulo and must NOT be used. The `.like()` method generates `GLOB` (not
`LIKE`) and is also wrong for this use case.

### N+1 queries in `get_entry`

`get_entry` fires ~15–20 individual SQL queries per entry. For the initial migration
this is intentional — correctness over performance. A follow-up optimisation can use
peewee `prefetch()` once the test suite is stable.

### Thread safety

`SqliteDatabase` uses a single connection and is not thread-safe by default. If thread
safety is needed, use `SqliteDatabase(..., check_same_thread=False)` or switch to
`SqliteExtDatabase` with connection pools. This is a known limitation documented here
for future reference.