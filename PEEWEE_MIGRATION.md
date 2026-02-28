# Peewee Migration Plan: `jmdict_sqlite.py` → `jmdict_peewee.py`

## Background

`jmdict_sqlite.py` currently uses [puchikarui](https://github.com/letuananh/puchikarui) as its
database abstraction layer. puchikarui has been unmaintained for over 5 years and `MemorySource`
(used for `memory_mode=True`) no longer exists in the installed version — meaning that feature is
silently broken today.

This migration replaces puchikarui with [peewee](https://docs.peewee-orm.com/) in a new parallel
file `jmdict_peewee.py`, preserving the exact same public interface as `jmdict_sqlite.py` so that
`util.py` can swap implementations without any changes to the rest of the codebase.

The existing `jmdict_sqlite.py` is left untouched until the new implementation is verified.

`memory_mode` is **not** supported in the peewee implementation. It was already silently broken
in puchikarui (`MemorySource` does not exist in the installed version) and is out of scope for
this migration. The `:memory:` path (empty in-memory DB for tests and one-shot imports) is fully
supported — only the "copy on-disk DB into RAM" behaviour is dropped.

---

## Key Differences to Understand Before Starting

### `:memory:` support

`:memory:` — SQLite creates a fresh, empty in-memory database. This is what the tests use; data
is imported from XML each test run. This continues to work identically in the peewee
implementation.

### `buckmode` → `peewee atomic()`

The bulk import path in `util.py` calls `ctx.buckmode()` and sets `ctx.auto_commit = False` to
batch writes. In peewee this becomes a single `database.atomic()` context manager combined with
PRAGMAs (`journal_mode=MEMORY`, `cache_size`, `temp_store=MEMORY`), which is more idiomatic and
equally fast.

### Context passing (`ctx=None` pattern)

Every method in `jmdict_sqlite.py` accepts an optional `ctx` argument and recurses with a managed
context when none is supplied. peewee manages connections at the `Database` object level; explicit
context passing is not required. However, to keep the public interface identical (so `util.py`
passes `ctx=` without errors), all public methods will accept and silently ignore a `ctx` keyword
argument where peewee handles connection management internally.

---

## Public Interface Contract

The following is what `jmdict_peewee.py` must expose, matching `jmdict_sqlite.py` exactly:

```python
class JMDictSQLite:
    def __init__(self, db_path: str, *args, **kwargs): ...

    # Metadata
    def update_jmd_meta(self, version: str, url: str, ctx=None): ...

    # Query
    def all_pos(self, ctx=None) -> list[str]: ...
    def search(self, query: str, ctx=None, pos=None, **kwargs) -> list[JMDEntry]: ...
    def search_iter(self, query: str, ctx=None, pos=None, **kwargs) -> Iterator[JMDEntry]: ...
    def get_entry(self, idseq: int, ctx=None) -> JMDEntry: ...

    # Import
    def insert_entries(self, entries, ctx=None): ...
    def insert_entry(self, entry: JMDEntry, ctx=None): ...

    # Table access (used directly in tests and util.py)
    # self.Entry.select()  →  peewee model select()
    # self.meta            →  peewee model for Meta table

class JamdictSQLite(JMDictSQLite):
    """Alias used internally by util.py (lowercase 'd')."""
    pass
```

The `self.Entry.select()` and `self.meta` attribute-style access from the test suite will be
handled by exposing model classes as instance attributes on `JMDictSQLite`.

---

## File Layout After Migration

```
jamdict/jamdict/
    jmdict_sqlite.py          ← unchanged (puchikarui, kept for comparison)
    jmdict_peewee.py          ← new file (peewee implementation, same interface)

jamdict/test/
    test_jmdict_sqlite.py     ← unchanged (existing tests against puchikarui impl)
    test_jmdict_peewee.py     ← new file (mirrors test_jmdict_sqlite.py exactly,
                                           imports from jmdict_peewee instead)
```

---

## Task List

### Phase 1 — Setup

- [x] **1.1** Add `peewee` to `pyproject.toml` dependencies.
- [x] **1.2** Confirm `peewee` installs cleanly in the `.venv` and imports without error.

---

### Phase 2 — Define Peewee Models in `jmdict_peewee.py`

Create `jamdict/jamdict/jmdict_peewee.py`. The peewee models replace both the `add_table(...)`
declarations in `JMDictSchema.__init__` and the external `setup_jmdict.sql` DDL file. The `.sql`
file is **not** modified or deleted yet — that happens only after the migration is verified end
to end.

Each table in the current schema maps to one peewee `Model` subclass:

- [x] **2.1** Define a module-level `database = SqliteDatabase(None)` proxy (deferred init).
  Using a deferred database means the same model classes work for both file-backed and `:memory:`
  databases without any additional plumbing.

- [x] **2.2** Define a `BaseModel(Model)` with `Meta.database = database`.

- [x] **2.3** Define the following model classes (one per table):

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

- [x] **2.4** Write `ALL_MODELS` — a list of all model classes used for `create_tables()` and
  `drop_tables()` calls.

---

### Phase 3 — Implement `JMDictSQLite` class

- [ ] **3.1** Implement `__init__(self, db_path, *args, **kwargs)`:
  - If `db_path == ":memory:"`, call `database.init(':memory:')`.
  - Otherwise call `database.init(db_path)`.
  - Call `database.connect(reuse_if_open=True)`.
  - Call `database.create_tables(ALL_MODELS, safe=True)` to create schema if it doesn't exist.
  - Expose `self.Entry = EntryModel` and `self.meta = MetaModel` as instance attributes so
    existing call sites like `self.db.Entry.select()` and `self.db.meta.select()` continue to
    work unchanged.

- [ ] **3.2** Implement `update_jmd_meta(self, version, url, ctx=None)`:
  - Upsert `jmdict.version` and `jmdict.url` rows in `MetaModel`.
  - `ctx` is accepted but ignored (peewee manages the connection).

- [ ] **3.3** Implement `all_pos(self, ctx=None) -> list[str]`:
  - Return `[row.text for row in PosModel.select(PosModel.text).distinct()]`.

- [ ] **3.4** Implement `_build_search_query(self, query, pos=None)`:
  - Mirror the logic from `jmdict_sqlite.py` exactly:
    - `id#<n>` → filter by `EntryModel.idseq`.
    - Wildcard (`%`, `_`, `@`) → use peewee LIKE expressions.
    - Exact → use `==` expressions.
    - `pos` filter → additional `idseq IN (subquery on PosModel)`.
  - Return a peewee `SelectQuery` on `EntryModel` rather than raw SQL strings.

- [ ] **3.5** Implement `search(self, query, ctx=None, pos=None, **kwargs) -> list[JMDEntry]`:
  - Use `_build_search_query` to get a queryset of `idseq` values.
  - Call `get_entry(idseq)` for each and return as a list.

- [ ] **3.6** Implement `search_iter(self, query, ctx=None, pos=None, **kwargs)`:
  - Same as `search` but `yield` each entry instead of collecting into a list.

- [ ] **3.7** Implement `get_entry(self, idseq, ctx=None) -> JMDEntry`:
  - Reconstruct the full `JMDEntry` domain object from peewee models.
  - Preserve the N-level assembly (links/bibs/etym/audit → kanji → kana → senses with all
    sub-tables) exactly as in the existing `get_entry`.
  - This is the most complex method; see the note on N+1 queries below.

- [ ] **3.8** Implement `insert_entries(self, entries, ctx=None)`:
  - Wrap in `database.atomic()` for batch performance (replaces `buckmode`).
  - Iterate and call `insert_entry` for each.

- [ ] **3.9** Implement `insert_entry(self, entry, ctx=None)`:
  - Mirror the field-by-field insert logic from `jmdict_sqlite.py`.
  - Use peewee `Model.create(...)` or `Model.insert(...)` calls.

- [ ] **3.10** Add `class JamdictSQLite(JMDictSQLite): pass` at the bottom of the file as an
  alias (this is what `util.py` actually instantiates).

---

### Phase 4 — Write `test/test_jmdict_peewee.py`

Mirror `test/test_jmdict_sqlite.py` exactly. The only changes are:

- [ ] **4.1** Change the import:
  ```python
  # from jamdict import JMDictSQLite               ← puchikarui version
  from jamdict.jmdict_peewee import JMDictSQLite   # peewee version
  ```
- [ ] **4.2** Use a separate `TEST_DB` path (`test_peewee.db`) to avoid collisions with the
  existing test database.
- [ ] **4.3** Mirror all five test methods with identical assertions:
  - `test_xml2sqlite` — import XML into file-backed DB, assert entry count and field values.
  - `test_import_to_ram` — import XML into `:memory:` DB, assert entry count.
  - `test_import_function` — exercise `Jamdict(db_file=":memory:", ...)` end-to-end import.
  - `test_search` — kana search, wildcard kanji search, meaning search, assert counts.
  - `test_iter_search` — iterate `search_iter`, collect kana forms, assert expected subset.

---

### Phase 5 — Integration Verification

These steps verify the peewee implementation works as a drop-in inside `util.py` without
permanently changing `util.py` yet.

- [ ] **5.1** In a local branch / scratch script, temporarily edit `util.py` to import
  `JamdictSQLite` from `jmdict_peewee` instead of `jmdict_sqlite`, then run the full test suite:
  ```
  .venv/bin/python -m pytest test/ -v
  ```
- [ ] **5.2** Confirm all tests in `test_jamdict.py` and `test_jmdict_peewee.py` pass.
- [ ] **5.3** Confirm `test_jmdict_sqlite.py` still passes (i.e. the puchikarui path is unbroken).

---

### Phase 6 — Cleanup (do after Phase 5 is fully green)

These tasks are explicitly deferred until the new implementation is trusted.

- [ ] **6.1** Update `util.py` to import from `jmdict_peewee` permanently.
- [ ] **6.2** Remove the `MemorySource` / `_MEMORY_MODE` try/except block and all `memory_mode`
  branching from `util.py`.
- [ ] **6.3** Update `__init__.py` to export the peewee-backed `JMDictSQLite`.
- [ ] **6.4** Remove `jmdict_sqlite.py` (or keep it archived).
- [ ] **6.5** Remove the now-redundant `data/setup_jmdict.sql` DDL file (schema is now defined
  entirely in the peewee models).
- [ ] **6.6** Remove the `puchikarui` dependency from `pyproject.toml` (only if no other file
  still uses it — check `kanjidic2_sqlite.py` and `jmnedict_sqlite.py` which are out of scope
  for this migration).
- [ ] **6.7** Update `tools.py` to remove the `puchikarui_version` import and `show_info` output
  line.
- [ ] **6.8** Update `PEEWEE_MIGRATION.md` to mark the migration complete.

---

## Notes and Gotchas

### N+1 queries in `get_entry`

The current `get_entry` fires roughly 15–20 individual SQL queries per entry (one per sub-table).
For the initial migration, **replicate this behaviour exactly** — correctness over performance.
Once the test suite is green, `get_entry` can be optimised with peewee `prefetch()` as a
separate follow-up task.

### Thread safety

puchikarui creates a new `sqlite3` connection per `ExecutionContext`. peewee's default
`SqliteDatabase` uses a single connection and is not thread-safe by default. If thread safety
is needed (e.g. in a web context), use `SqliteDatabase(..., check_same_thread=False)` or switch
to connection pools. For now, match the existing behaviour and document this as a known
limitation.

### `self.Entry.select()` in tests

`test_xml2sqlite` calls `self.db.Entry.select()` directly to count rows. The peewee
`EntryModel.select()` returns a `ModelSelect` (lazy). Wrap it in `list()` or call `.count()`
to materialise. The test asserts `len(entries) == len(self.xdb)` so `list(EntryModel.select())`
works correctly.

### `ctx` parameter compatibility

`util.py` passes a shared `ctx` object into methods like `search(query, pos=pos, ctx=ctx)` so
that all queries within a single `lookup()` call share one SQLite connection (for performance
and consistency). In the peewee implementation `ctx` is ignored — peewee handles connection
reuse transparently via the `Database` object. Accept `**kwargs` or explicit `ctx=None` on every
public method signature so call sites don't need to change.

### Deferred database initialisation

Using `SqliteDatabase(None)` (deferred) at module level means the same model classes work for
both file-backed and `:memory:` databases — which is exactly how the test suite works (one
file-backed instance, one `:memory:` instance). Call `database.init(path)` inside `__init__`
before connecting.
