# -*- coding: utf-8 -*-

"""
JMDict SQLite backend — peewee implementation.

Each JMDictDB instance owns its own SqliteDatabase object.  Model classes are
unbound at definition time (database=None) and bound to a specific database
instance via peewee's bind() API inside every public method.  This means
multiple JMDictDB instances with different paths — including :memory: — can
coexist safely in the same process without stomping on each other.

This module is intentionally self-contained.  It does NOT attempt to replicate
the puchikarui ctx-passing convention used by jmdict_sqlite.py; call sites
should use the clean API defined here.
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
    Audit,
    BibInfo,
    EntryInfo,
    JMDEntry,
    KanaForm,
    KanjiForm,
    Link,
    LSource,
    Sense,
    SenseGloss,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

JMDICT_VERSION = "1.08"
JMDICT_URL = "http://www.csse.monash.edu.au/~jwb/edict.html"


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model definitions — database=None (unbound)
#
# Models are defined once at module level with no database attached.
# JMDictDB.__init__ calls db.bind(ALL_MODELS) so each instance gets its
# own connection without interfering with any other instance.
# ---------------------------------------------------------------------------


class _Base(Model):
    # No Meta.database needed — peewee defaults to database=None when unset.
    # Queries are routed per-instance via bind_ctx() inside every JMDictDB method.
    pass


class MetaModel(_Base):
    key = CharField(primary_key=True)
    value = TextField()

    class Meta:
        table_name = "meta"


class EntryModel(_Base):
    idseq = IntegerField(primary_key=True)

    class Meta:
        table_name = "Entry"


class LinkModel(_Base):
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="links")
    tag = TextField(null=True)
    desc = TextField(null=True)
    uri = TextField(null=True)

    class Meta:
        table_name = "Link"


class BibModel(_Base):
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="bibs")
    tag = TextField(null=True)
    text = TextField(null=True)

    class Meta:
        table_name = "Bib"


class EtymModel(_Base):
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="etyoms")
    text = TextField(null=True)

    class Meta:
        table_name = "Etym"
        primary_key = False


class AuditModel(_Base):
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="audits")
    upd_date = TextField(null=True)
    upd_detl = TextField(null=True)

    class Meta:
        table_name = "Audit"
        primary_key = False


class KanjiModel(_Base):
    id = AutoField()
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="kanjis")
    text = TextField(null=True)

    class Meta:
        table_name = "Kanji"


class KJIModel(_Base):
    kid = ForeignKeyField(KanjiModel, column_name="kid", backref="infos")
    text = TextField(null=True)

    class Meta:
        table_name = "KJI"
        primary_key = False


class KJPModel(_Base):
    kid = ForeignKeyField(KanjiModel, column_name="kid", backref="pris")
    text = TextField(null=True)

    class Meta:
        table_name = "KJP"
        primary_key = False


class KanaModel(_Base):
    id = AutoField()
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="kanas")
    text = TextField(null=True)
    nokanji = BooleanField(null=True)

    class Meta:
        table_name = "Kana"


class KNIModel(_Base):
    kid = ForeignKeyField(KanaModel, column_name="kid", backref="infos")
    text = TextField(null=True)

    class Meta:
        table_name = "KNI"
        primary_key = False


class KNPModel(_Base):
    kid = ForeignKeyField(KanaModel, column_name="kid", backref="pris")
    text = TextField(null=True)

    class Meta:
        table_name = "KNP"
        primary_key = False


class KNRModel(_Base):
    kid = ForeignKeyField(KanaModel, column_name="kid", backref="restrs")
    text = TextField(null=True)

    class Meta:
        table_name = "KNR"
        primary_key = False


class SenseModel(_Base):
    id = AutoField()
    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="senses")

    class Meta:
        table_name = "Sense"


class StagkModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="stagks")
    text = TextField(null=True)

    class Meta:
        table_name = "stagk"
        primary_key = False


class StagrModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="stagrs")
    text = TextField(null=True)

    class Meta:
        table_name = "stagr"
        primary_key = False


class PosModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="poses")
    text = TextField(null=True)

    class Meta:
        table_name = "pos"
        primary_key = False


class XrefModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="xrefs")
    text = TextField(null=True)

    class Meta:
        table_name = "xref"
        primary_key = False


class AntonymModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="antonyms")
    text = TextField(null=True)

    class Meta:
        table_name = "antonym"
        primary_key = False


class FieldModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="fields")
    text = TextField(null=True)

    class Meta:
        table_name = "field"
        primary_key = False


class MiscModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="miscs")
    text = TextField(null=True)

    class Meta:
        table_name = "misc"
        primary_key = False


class SenseInfoModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="sense_infos")
    text = TextField(null=True)

    class Meta:
        table_name = "SenseInfo"
        primary_key = False


class SenseSourceModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="sense_sources")
    text = TextField(null=True)
    lang = TextField(null=True)
    lstype = TextField(null=True)
    wasei = TextField(null=True)

    class Meta:
        table_name = "SenseSource"
        primary_key = False


class DialectModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="dialects")
    text = TextField(null=True)

    class Meta:
        table_name = "dialect"
        primary_key = False


class SenseGlossModel(_Base):
    sid = ForeignKeyField(SenseModel, column_name="sid", backref="glosses")
    lang = TextField(null=True)
    gend = TextField(null=True)
    text = TextField(null=True)

    class Meta:
        table_name = "SenseGloss"
        primary_key = False


# Ordered so parent tables are created before child tables.
ALL_MODELS = [
    MetaModel,
    EntryModel,
    LinkModel,
    BibModel,
    EtymModel,
    AuditModel,
    KanjiModel,
    KJIModel,
    KJPModel,
    KanaModel,
    KNIModel,
    KNPModel,
    KNRModel,
    SenseModel,
    StagkModel,
    StagrModel,
    PosModel,
    XrefModel,
    AntonymModel,
    FieldModel,
    MiscModel,
    SenseInfoModel,
    SenseSourceModel,
    DialectModel,
    SenseGlossModel,
]


# ---------------------------------------------------------------------------
# JMDictDB — the clean public API
# ---------------------------------------------------------------------------


class JMDictDB:
    """
    peewee-backed JMDict SQLite store.

    Each instance owns its own SqliteDatabase connection.  Multiple instances
    with different paths (including ':memory:') can coexist in the same
    process.

    Typical usage::

        db = JMDictDB("path/to/jmdict.db")

        # Import from parsed XML entries
        db.insert_entries(xml_entries)

        # Query
        results: list[JMDEntry] = db.search("食べる")
        entry:   JMDEntry       = db.get_entry(1234567)

        # Iterate (memory-efficient for large result sets)
        for entry in db.search_iter("食べ%る"):
            print(entry)

        # Metadata
        db.update_meta(version="1.08", url="http://...")
        pos_list: list[str] = db.all_pos()
    """

    KEY_VERSION = "jmdict.version"
    KEY_URL = "jmdict.url"

    def __init__(self, db_path: str):
        """
        Open (or create) a JMDict SQLite database at *db_path*.

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
        # We do NOT call db.bind() permanently — that would mutate the
        # module-level model classes and break any other JMDictDB instance.
        # Instead, every public method wraps its queries in
        # self._db.bind_ctx(ALL_MODELS), which temporarily routes model
        # operations to this instance's database without touching others.
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
            (self.KEY_VERSION, JMDICT_VERSION),
            (self.KEY_URL, JMDICT_URL),
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

    def update_meta(self, version: str, url: str) -> None:
        """Upsert the jmdict version and source URL in the meta table."""
        with self._db.bind_ctx(ALL_MODELS):
            with self._db.atomic():
                MetaModel.insert(key=self.KEY_VERSION, value=version).on_conflict(
                    conflict_target=[MetaModel.key],
                    update={MetaModel.value: version},
                ).execute()
                MetaModel.insert(key=self.KEY_URL, value=url).on_conflict(
                    conflict_target=[MetaModel.key],
                    update={MetaModel.value: url},
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
    # Part-of-speech
    # ------------------------------------------------------------------

    def all_pos(self) -> List[str]:
        """Return a list of all distinct POS tags stored in the database."""
        with self._db.bind_ctx(ALL_MODELS):
            return [row.text for row in PosModel.select(PosModel.text).distinct()]

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _build_entry_query(self, query: str, pos=None):
        """
        Build a peewee SelectQuery of EntryModel rows for the given query.

        Supported query forms:

        * ``'id#<n>'``    — look up by numeric idseq
        * ``'%'`` / ``''`` — (with pos filter) match everything
        * wildcard (contains ``%``, ``_``, or ``@``) — SQL LIKE  (peewee ``**``)
        * exact string — equality match across kanji, kana, gloss

        NOTE: this method only *builds* the query object; it does not execute
        it.  The caller must wrap execution inside ``bind_ctx``.
        """
        q = EntryModel.select()

        if query.startswith("id#"):
            try:
                idseq = int(query[3:])
            except ValueError:
                return q.where(EntryModel.idseq == -1)  # no results
            if idseq >= 0:
                q = q.where(EntryModel.idseq == idseq)
            return q

        if query and query != "%":
            is_wildcard = "%" in query or "_" in query or "@" in query

            if is_wildcard:
                # peewee ** operator → SQL LIKE (case-insensitive on ASCII,
                # but for Japanese text that distinction is irrelevant)
                kanji_sq = KanjiModel.select(KanjiModel.idseq).where(
                    KanjiModel.text**query
                )
                kana_sq = KanaModel.select(KanaModel.idseq).where(KanaModel.text**query)
                gloss_sq = (
                    SenseModel.select(SenseModel.idseq)
                    .join(SenseGlossModel, on=(SenseGlossModel.sid == SenseModel.id))
                    .where(SenseGlossModel.text**query)
                )
            else:
                kanji_sq = KanjiModel.select(KanjiModel.idseq).where(
                    KanjiModel.text == query
                )
                kana_sq = KanaModel.select(KanaModel.idseq).where(
                    KanaModel.text == query
                )
                gloss_sq = (
                    SenseModel.select(SenseModel.idseq)
                    .join(SenseGlossModel, on=(SenseGlossModel.sid == SenseModel.id))
                    .where(SenseGlossModel.text == query)
                )

            q = q.where(
                (EntryModel.idseq << kanji_sq)
                | (EntryModel.idseq << kana_sq)
                | (EntryModel.idseq << gloss_sq)
            )

        if pos:
            if isinstance(pos, str):
                getLogger().warning(
                    "pos filter should be a list, not a string — wrapping"
                )
                pos = [pos]
            pos_sq = (
                SenseModel.select(SenseModel.idseq)
                .join(PosModel, on=(PosModel.sid == SenseModel.id))
                .where(PosModel.text.in_(pos))
            )
            q = q.where(EntryModel.idseq << pos_sq)

        return q

    def search(self, query: str, pos=None) -> List[JMDEntry]:
        """Return all entries matching *query* as a list."""
        return list(self.search_iter(query, pos=pos))

    def search_iter(self, query: str, pos=None) -> Iterator[JMDEntry]:
        """Yield entries matching *query* one at a time."""
        with self._db.bind_ctx(ALL_MODELS):
            idseqs = [row.idseq for row in self._build_entry_query(query, pos=pos)]
        for idseq in idseqs:
            entry = self.get_entry(idseq)
            if entry is not None:
                yield entry

    def get_entry(self, idseq: int) -> Optional[JMDEntry]:
        """
        Reconstruct a full JMDEntry domain object from the database.

        Returns None if no entry with the given idseq exists.
        """
        with self._db.bind_ctx(ALL_MODELS):
            if not EntryModel.select().where(EntryModel.idseq == idseq).exists():
                return None

            entry = JMDEntry(str(idseq))

            # ---- entry-level info (links / bibs / etym / audit) ---------
            links = list(LinkModel.select().where(LinkModel.idseq == idseq))
            bibs = list(BibModel.select().where(BibModel.idseq == idseq))
            etyoms = list(EtymModel.select().where(EtymModel.idseq == idseq))
            audits = list(AuditModel.select().where(AuditModel.idseq == idseq))

            if links or bibs or etyoms or audits:
                entry.info = EntryInfo()
                for lnk in links:
                    entry.info.links.append(Link(lnk.tag, lnk.desc, lnk.uri))
                for bib in bibs:
                    entry.info.bibinfo.append(BibInfo(bib.tag, bib.text))
                for etym in etyoms:
                    entry.info.etym.append(etym.text)
                for aud in audits:
                    entry.info.audit.append(Audit(aud.upd_date, aud.upd_detl))

            # ---- kanji forms --------------------------------------------
            for dbkj in KanjiModel.select().where(KanjiModel.idseq == idseq):
                kj = KanjiForm(dbkj.text)
                for row in KJIModel.select().where(KJIModel.kid == dbkj.id):
                    kj.info.append(row.text)
                for row in KJPModel.select().where(KJPModel.kid == dbkj.id):
                    kj.pri.append(row.text)
                entry.kanji_forms.append(kj)

            # ---- kana forms ---------------------------------------------
            for dbkn in KanaModel.select().where(KanaModel.idseq == idseq):
                kn = KanaForm(dbkn.text, dbkn.nokanji)
                for row in KNIModel.select().where(KNIModel.kid == dbkn.id):
                    kn.info.append(row.text)
                for row in KNPModel.select().where(KNPModel.kid == dbkn.id):
                    kn.pri.append(row.text)
                for row in KNRModel.select().where(KNRModel.kid == dbkn.id):
                    kn.restr.append(row.text)
                entry.kana_forms.append(kn)

            # ---- senses -------------------------------------------------
            for dbs in SenseModel.select().where(SenseModel.idseq == idseq):
                s = Sense()
                sid = dbs.id
                for row in StagkModel.select().where(StagkModel.sid == sid):
                    s.stagk.append(row.text)
                for row in StagrModel.select().where(StagrModel.sid == sid):
                    s.stagr.append(row.text)
                for row in PosModel.select().where(PosModel.sid == sid):
                    s.pos.append(row.text)
                for row in XrefModel.select().where(XrefModel.sid == sid):
                    s.xref.append(row.text)
                for row in AntonymModel.select().where(AntonymModel.sid == sid):
                    s.antonym.append(row.text)
                for row in FieldModel.select().where(FieldModel.sid == sid):
                    s.field.append(row.text)
                for row in MiscModel.select().where(MiscModel.sid == sid):
                    s.misc.append(row.text)
                for row in SenseInfoModel.select().where(SenseInfoModel.sid == sid):
                    s.info.append(row.text)
                for row in SenseSourceModel.select().where(SenseSourceModel.sid == sid):
                    s.lsource.append(LSource(row.lang, row.lstype, row.wasei, row.text))
                for row in DialectModel.select().where(DialectModel.sid == sid):
                    s.dialect.append(row.text)
                for row in SenseGlossModel.select().where(SenseGlossModel.sid == sid):
                    s.gloss.append(SenseGloss(row.lang, row.gend, row.text))
                entry.senses.append(s)

        return entry

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def insert_entries(self, entries) -> None:
        """
        Bulk-insert a collection of JMDEntry objects.

        Wraps the entire operation in a single transaction with performance
        PRAGMAs to match the throughput of the original puchikarui buckmode.
        """
        getLogger().debug("JMDictDB: bulk insert %d entries", len(entries))
        with self._db.bind_ctx(ALL_MODELS):
            if self._db_path != ":memory:":
                self._db.execute_sql("PRAGMA journal_mode=MEMORY")
            self._db.execute_sql("PRAGMA cache_size=-65536")  # 64 MB page cache
            self._db.execute_sql("PRAGMA temp_store=MEMORY")
            with self._db.atomic():
                for entry in entries:
                    self._insert_entry_unsafe(entry)

    def insert_entry(self, entry: JMDEntry) -> None:
        """Insert a single JMDEntry and all its child rows."""
        with self._db.bind_ctx(ALL_MODELS):
            self._insert_entry_unsafe(entry)

    def _insert_entry_unsafe(self, entry: JMDEntry) -> None:
        """
        Insert a single JMDEntry without acquiring bind_ctx.

        Must only be called from within an active bind_ctx block.
        """
        EntryModel.create(idseq=entry.idseq)

        # ---- entry info ---------------------------------------------
        if entry.info:
            for lnk in entry.info.links:
                LinkModel.create(
                    idseq=entry.idseq, tag=lnk.tag, desc=lnk.desc, uri=lnk.uri
                )
            for bib in entry.info.bibinfo:
                BibModel.create(idseq=entry.idseq, tag=bib.tag, text=bib.text)
            for etym in entry.info.etym:
                EtymModel.create(idseq=entry.idseq, text=etym)
            for aud in entry.info.audit:
                AuditModel.create(
                    idseq=entry.idseq, upd_date=aud.upd_date, upd_detl=aud.upd_detl
                )

        # ---- kanji forms --------------------------------------------
        for kj in entry.kanji_forms:
            dbkj = KanjiModel.create(idseq=entry.idseq, text=kj.text)
            for info in kj.info:
                KJIModel.create(kid=dbkj.id, text=info)
            for pri in kj.pri:
                KJPModel.create(kid=dbkj.id, text=pri)

        # ---- kana forms ---------------------------------------------
        for kn in entry.kana_forms:
            dbkn = KanaModel.create(idseq=entry.idseq, text=kn.text, nokanji=kn.nokanji)
            for info in kn.info:
                KNIModel.create(kid=dbkn.id, text=info)
            for pri in kn.pri:
                KNPModel.create(kid=dbkn.id, text=pri)
            for restr in kn.restr:
                KNRModel.create(kid=dbkn.id, text=restr)

        # ---- senses -------------------------------------------------
        for s in entry.senses:
            dbs = SenseModel.create(idseq=entry.idseq)
            sid = dbs.id
            for text in s.stagk:
                StagkModel.create(sid=sid, text=text)
            for text in s.stagr:
                StagrModel.create(sid=sid, text=text)
            for text in s.pos:
                PosModel.create(sid=sid, text=text)
            for text in s.xref:
                XrefModel.create(sid=sid, text=text)
            for text in s.antonym:
                AntonymModel.create(sid=sid, text=text)
            for text in s.field:
                FieldModel.create(sid=sid, text=text)
            for text in s.misc:
                MiscModel.create(sid=sid, text=text)
            for text in s.info:
                SenseInfoModel.create(sid=sid, text=text)
            for ls in s.lsource:
                SenseSourceModel.create(
                    sid=sid,
                    text=ls.text,
                    lang=ls.lang,
                    lstype=ls.lstype,
                    wasei=ls.wasei,
                )
            for text in s.dialect:
                DialectModel.create(sid=sid, text=text)
            for g in s.gloss:
                SenseGlossModel.create(sid=sid, lang=g.lang, gend=g.gend, text=g.text)

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
        return f"JMDictDB({self._db_path!r})"
