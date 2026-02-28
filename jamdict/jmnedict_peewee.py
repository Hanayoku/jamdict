# -*- coding: utf-8 -*-

"""
JMNEDict SQLite backend — peewee implementation.

Each JMNEDictDB instance owns its own SqliteDatabase object.  Model classes
are unbound at definition time (database=None) and bound to a specific
database instance via peewee's bind_ctx API inside every public method.  This
means multiple JMNEDictDB instances with different paths — including :memory:
— can coexist safely in the same process without stomping on each other.

This module mirrors the design established by jmdict_peewee.py and is
intentionally self-contained.
"""

# This code is a part of jamdict library: https://github.com/neocl/jamdict
# :copyright: (c) 2016 Le Tuan Anh <tuananh.ke@gmail.com>
# :license: MIT, see LICENSE for more details.

import logging
import os
from typing import Iterator, List, Optional

from peewee import (
    AutoField,
    BooleanField,
    CharField,
    ForeignKeyField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
)

from . import __url__ as JAMDICT_URL
from . import __version__ as JAMDICT_VERSION
from .jmdict import (
    JMDEntry,
    KanaForm,
    KanjiForm,
    SenseGloss,
    Translation,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

JMNEDICT_VERSION = "1.08"
JMNEDICT_URL = "https://www.edrdg.org/enamdict/enamdict_doc.html"
JMNEDICT_DATE = "2020-05-29"


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model definitions — database=None (unbound)
#
# Models are defined once at module level with no database attached.
# JMNEDictDB.__init__ uses bind_ctx so each instance gets its own
# connection without interfering with any other instance.
# ---------------------------------------------------------------------------


class _Base(Model):
    pass


class MetaModel(_Base):
    key = CharField(primary_key=True)
    value = TextField()

    class Meta:
        table_name = "meta"


class NEEntryModel(_Base):
    idseq = IntegerField(primary_key=True)

    class Meta:
        table_name = "NEEntry"


class NEKanjiModel(_Base):
    ID = AutoField()
    idseq = ForeignKeyField(NEEntryModel, column_name="idseq", backref="kanji_forms")
    text = TextField(null=True)

    class Meta:
        table_name = "NEKanji"


class NEKanaModel(_Base):
    ID = AutoField()
    idseq = ForeignKeyField(NEEntryModel, column_name="idseq", backref="kana_forms")
    text = TextField(null=True)
    nokanji = BooleanField(null=True)

    class Meta:
        table_name = "NEKana"


class NETranslationModel(_Base):
    ID = AutoField()
    idseq = ForeignKeyField(NEEntryModel, column_name="idseq", backref="translations")

    class Meta:
        table_name = "NETranslation"


class NETransTypeModel(_Base):
    tid = ForeignKeyField(NETranslationModel, column_name="tid", backref="name_types")
    text = TextField(null=True)

    class Meta:
        table_name = "NETransType"
        primary_key = False


class NETransXRefModel(_Base):
    tid = ForeignKeyField(NETranslationModel, column_name="tid", backref="xrefs")
    text = TextField(null=True)

    class Meta:
        table_name = "NETransXRef"
        primary_key = False


class NETransGlossModel(_Base):
    tid = ForeignKeyField(NETranslationModel, column_name="tid", backref="glosses")
    lang = TextField(null=True)
    gend = TextField(null=True)
    text = TextField(null=True)

    class Meta:
        table_name = "NETransGloss"
        primary_key = False


# Ordered so parent tables are created before child tables.
ALL_MODELS = [
    MetaModel,
    NEEntryModel,
    NEKanjiModel,
    NEKanaModel,
    NETranslationModel,
    NETransTypeModel,
    NETransXRefModel,
    NETransGlossModel,
]


# ---------------------------------------------------------------------------
# JMNEDictDB — the clean public API
# ---------------------------------------------------------------------------


class JMNEDictDB:
    """
    peewee-backed JMNEDict SQLite store.

    Each instance owns its own SqliteDatabase connection.  Multiple instances
    with different paths (including ':memory:') can coexist in the same
    process.

    Typical usage::

        db = JMNEDictDB("path/to/jmnedict.db")

        # Import from parsed XML entries
        db.insert_entries(xml_entries)

        # Query
        entry  = db.get_ne(1234567)             # → JMDEntry | None
        results = db.search_ne("神龍")           # → list[JMDEntry]
        results = db.search_ne("%神%")           # wildcard LIKE
        results = db.search_ne("id#5741815")    # by idseq
        for e in db.search_ne_iter("神%"):      # memory-efficient iteration
            print(e)

        # Metadata
        db.all_ne_type()                         # → list[str]
        db.update_meta(version, url, date)
        db.get_meta("jmnedict.version")          # → str | None

        # Resource management
        db.close()
        with JMNEDictDB(":memory:") as db:
            ...
    """

    KEY_VERSION = "jmnedict.version"
    KEY_URL = "jmnedict.url"
    KEY_DATE = "jmnedict.date"

    def __init__(self, db_path: str):
        """
        Open (or create) a JMNEDict SQLite database at *db_path*.

        Pass ``':memory:'`` for a fresh in-process database that is discarded
        when the instance is garbage-collected.
        """
        if db_path and db_path != ":memory:":
            db_path = os.path.abspath(os.path.expanduser(db_path))
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self._db_path = db_path
        self._db = SqliteDatabase(
            db_path,
            pragmas={"foreign_keys": 0},
        )
        with self._db.bind_ctx(ALL_MODELS):
            self._db.connect(reuse_if_open=True)
            self._db.create_tables(ALL_MODELS, safe=True)
        self._seed_meta()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _seed_meta(self) -> None:
        """Insert default metadata rows if they are absent."""
        defaults = [
            (self.KEY_VERSION, JMNEDICT_VERSION),
            (self.KEY_URL, JMNEDICT_URL),
            (self.KEY_DATE, JMNEDICT_DATE),
            ("generator", "jamdict"),
            ("generator_version", JAMDICT_VERSION),
            ("generator_url", JAMDICT_URL),
        ]
        with self._db.bind_ctx(ALL_MODELS):
            with self._db.atomic():
                for key, value in defaults:
                    MetaModel.get_or_create(key=key, defaults={"value": value})

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def update_meta(self, version: str, url: str, date: str = "") -> None:
        """Upsert the jmnedict version, source URL and date in the meta table."""
        rows = [
            (self.KEY_VERSION, version),
            (self.KEY_URL, url),
            (self.KEY_DATE, date),
        ]
        with self._db.bind_ctx(ALL_MODELS):
            with self._db.atomic():
                for key, value in rows:
                    MetaModel.insert(key=key, value=value).on_conflict(
                        conflict_target=[MetaModel.key],
                        update={MetaModel.value: value},
                    ).execute()

    def get_meta(self, key: str) -> Optional[str]:
        """Return the value for *key* from the meta table, or None."""
        with self._db.bind_ctx(ALL_MODELS):
            row = MetaModel.get_or_none(MetaModel.key == key)
        return row.value if row is not None else None

    def all_meta(self) -> List[tuple]:
        """Return all metadata rows as a list of ``(key, value)`` tuples."""
        with self._db.bind_ctx(ALL_MODELS):
            return [
                (row.key, row.value)
                for row in MetaModel.select().order_by(MetaModel.key)
            ]

    # ------------------------------------------------------------------
    # Part-of-speech / name types
    # ------------------------------------------------------------------

    def all_ne_type(self) -> List[str]:
        """Return a list of all distinct name-type tags in the database."""
        with self._db.bind_ctx(ALL_MODELS):
            return [
                row.text
                for row in NETransTypeModel.select(NETransTypeModel.text).distinct()
            ]

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _build_ne_search_query(self, query: str):
        """
        Build a peewee SelectQuery of NEEntryModel rows for the given query.

        Supported query forms:

        * ``'id#<n>'``    — look up by numeric idseq
        * wildcard (contains ``%``, ``_``, or ``@``) — SQL LIKE  (peewee ``**``)
        * exact string — equality match across kanji, kana, gloss and name_type

        NOTE: this method only *builds* the query object; the caller must wrap
        execution inside ``bind_ctx``.
        """
        q = NEEntryModel.select()

        if query.startswith("id#"):
            try:
                idseq = int(query[3:])
            except ValueError:
                return q.where(NEEntryModel.idseq == -1)
            if idseq >= 0:
                q = q.where(NEEntryModel.idseq == idseq)
            return q

        is_wildcard = "%" in query or "_" in query or "@" in query

        if is_wildcard:
            kanji_sq = NEKanjiModel.select(NEKanjiModel.idseq).where(
                NEKanjiModel.text**query
            )
            kana_sq = NEKanaModel.select(NEKanaModel.idseq).where(
                NEKanaModel.text**query
            )
            gloss_sq = (
                NETranslationModel.select(NETranslationModel.idseq)
                .join(
                    NETransGlossModel,
                    on=(NETransGlossModel.tid == NETranslationModel.ID),
                )
                .where(NETransGlossModel.text**query)
            )
            netype_sq = (
                NETranslationModel.select(NETranslationModel.idseq)
                .join(
                    NETransTypeModel,
                    on=(NETransTypeModel.tid == NETranslationModel.ID),
                )
                .where(NETransTypeModel.text**query)
            )
        else:
            kanji_sq = NEKanjiModel.select(NEKanjiModel.idseq).where(
                NEKanjiModel.text == query
            )
            kana_sq = NEKanaModel.select(NEKanaModel.idseq).where(
                NEKanaModel.text == query
            )
            gloss_sq = (
                NETranslationModel.select(NETranslationModel.idseq)
                .join(
                    NETransGlossModel,
                    on=(NETransGlossModel.tid == NETranslationModel.ID),
                )
                .where(NETransGlossModel.text == query)
            )
            netype_sq = (
                NETranslationModel.select(NETranslationModel.idseq)
                .join(
                    NETransTypeModel,
                    on=(NETransTypeModel.tid == NETranslationModel.ID),
                )
                .where(NETransTypeModel.text == query)
            )

        q = q.where(
            (NEEntryModel.idseq << kanji_sq)
            | (NEEntryModel.idseq << kana_sq)
            | (NEEntryModel.idseq << gloss_sq)
            | (NEEntryModel.idseq << netype_sq)
        )
        return q

    def search_ne(self, query: str) -> List[JMDEntry]:
        """Return all named-entity entries matching *query* as a list."""
        return list(self.search_ne_iter(query))

    def search_ne_iter(self, query: str) -> Iterator[JMDEntry]:
        """Yield named-entity entries matching *query* one at a time."""
        with self._db.bind_ctx(ALL_MODELS):
            idseqs = [row.idseq for row in self._build_ne_search_query(query)]
        for idseq in idseqs:
            entry = self.get_ne(idseq)
            if entry is not None:
                yield entry

    def get_ne(self, idseq: int) -> Optional[JMDEntry]:
        """
        Reconstruct a full JMDEntry domain object from the database.

        Returns None if no entry with the given idseq exists.
        """
        with self._db.bind_ctx(ALL_MODELS):
            if not NEEntryModel.select().where(NEEntryModel.idseq == idseq).exists():
                return None

            entry = JMDEntry(str(idseq))
            entry.idseq = idseq

            # ---- kanji forms ----------------------------------------
            for dbkj in NEKanjiModel.select().where(NEKanjiModel.idseq == idseq):
                kj = KanjiForm(dbkj.text)
                entry.kanji_forms.append(kj)

            # ---- kana forms -----------------------------------------
            for dbkn in NEKanaModel.select().where(NEKanaModel.idseq == idseq):
                kn = KanaForm(dbkn.text, dbkn.nokanji)
                entry.kana_forms.append(kn)

            # ---- translations (senses) ------------------------------
            for dbt in NETranslationModel.select().where(
                NETranslationModel.idseq == idseq
            ):
                t = Translation()
                tid = dbt.ID
                for nt_row in NETransTypeModel.select().where(
                    NETransTypeModel.tid == tid
                ):
                    t.name_type.append(nt_row.text)
                for xr_row in NETransXRefModel.select().where(
                    NETransXRefModel.tid == tid
                ):
                    t.xref.append(xr_row.text)
                for g_row in NETransGlossModel.select().where(
                    NETransGlossModel.tid == tid
                ):
                    t.gloss.append(SenseGloss(g_row.lang, g_row.gend, g_row.text))
                entry.senses.append(t)

        return entry

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def insert_entries(self, entries) -> None:
        """
        Bulk-insert a collection of JMDEntry objects (JMNEDict entries).

        Wraps the entire operation in a single transaction with performance
        PRAGMAs to match the throughput of the original puchikarui buckmode.
        """
        getLogger().debug("JMNEDictDB: bulk insert %d entries", len(entries))
        with self._db.bind_ctx(ALL_MODELS):
            if self._db_path != ":memory:":
                self._db.execute_sql("PRAGMA journal_mode=MEMORY")
            self._db.execute_sql("PRAGMA cache_size=-65536")
            self._db.execute_sql("PRAGMA temp_store=MEMORY")
            with self._db.atomic():
                for entry in entries:
                    self._insert_entry_unsafe(entry)

    def insert_entry(self, entry: JMDEntry) -> None:
        """Insert a single JMNEDict entry and all its child rows."""
        with self._db.bind_ctx(ALL_MODELS):
            self._insert_entry_unsafe(entry)

    def _insert_entry_unsafe(self, entry: JMDEntry) -> None:
        """
        Insert a single JMNEDict entry without acquiring bind_ctx.

        Must only be called from within an active bind_ctx block.
        """
        idseq = int(entry.idseq)
        NEEntryModel.create(idseq=idseq)

        # ---- kanji forms --------------------------------------------
        for kj in entry.kanji_forms:
            NEKanjiModel.create(idseq=idseq, text=kj.text)

        # ---- kana forms ---------------------------------------------
        for kn in entry.kana_forms:
            NEKanaModel.create(idseq=idseq, text=kn.text, nokanji=kn.nokanji)

        # ---- translations -------------------------------------------
        for s in entry.senses:
            t_row = NETranslationModel.create(idseq=idseq)
            tid = t_row.ID
            # name_type (only on Translation subclass)
            name_types = getattr(s, "name_type", [])
            for nt in name_types:
                NETransTypeModel.create(tid=tid, text=nt)
            # xref (only on Translation subclass)
            xrefs = getattr(s, "xref", [])
            for xr in xrefs:
                NETransXRefModel.create(tid=tid, text=xr)
            # gloss
            for g in s.gloss:
                NETransGlossModel.create(tid=tid, lang=g.lang, gend=g.gend, text=g.text)

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying database connection."""
        if not self._db.is_closed():
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __repr__(self) -> str:
        return f"JMNEDictDB({self._db_path!r})"
