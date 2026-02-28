# -*- coding: utf-8 -*-

"""
JMDict in SQLite format — peewee implementation.

Replaces the puchikarui-backed jmdict_sqlite.py with an identical public interface
backed by peewee.  The existing jmdict_sqlite.py is left untouched.
"""

# This code is a part of jamdict library: https://github.com/neocl/jamdict
# :copyright: (c) 2016 Le Tuan Anh <tuananh.ke@gmail.com>
# :license: MIT, see LICENSE for more details.

import logging
import os

from peewee import (
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

MY_FOLDER = os.path.dirname(os.path.abspath(__file__))
JMDICT_VERSION = "1.08"
JMDICT_URL = "http://www.csse.monash.edu.au/~jwb/edict.html"


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 2.1 — Deferred database proxy
#
# SqliteDatabase(None) defers initialisation so the same model classes work
# for both file-backed and :memory: databases.  Call database.init(path)
# inside __init__ before connecting.
# ---------------------------------------------------------------------------

database = SqliteDatabase(None)


# ---------------------------------------------------------------------------
# Phase 2.2 — BaseModel
# ---------------------------------------------------------------------------


class BaseModel(Model):
    class Meta:
        database = database


# ---------------------------------------------------------------------------
# Phase 2.3 — Model classes (one per table)
# ---------------------------------------------------------------------------


class MetaModel(BaseModel):
    """key/value metadata store (table: meta)."""

    key = CharField(primary_key=True)
    value = TextField()

    class Meta:
        table_name = "meta"


class EntryModel(BaseModel):
    """Top-level dictionary entry (table: Entry)."""

    idseq = IntegerField(primary_key=True)

    class Meta:
        table_name = "Entry"


class LinkModel(BaseModel):
    """Entry hyperlink info (table: Link)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="links")
    tag = TextField(null=True)
    desc = TextField(null=True)
    uri = TextField(null=True)

    class Meta:
        table_name = "Link"


class BibModel(BaseModel):
    """Entry bibliographic info (table: Bib)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="bibs")
    tag = TextField(null=True)
    text = TextField(null=True)

    class Meta:
        table_name = "Bib"


class EtymModel(BaseModel):
    """Entry etymology (table: Etym)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="etyoms")
    text = TextField(null=True)

    class Meta:
        table_name = "Etym"
        # Etym has no surrogate PK in the SQL schema; peewee adds an implicit
        # auto-increment 'id' column which is fine — the original schema also
        # has no PK on this table.
        primary_key = False


class AuditModel(BaseModel):
    """Entry audit trail (table: Audit)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="audits")
    upd_date = TextField(null=True)
    upd_detl = TextField(null=True)

    class Meta:
        table_name = "Audit"
        primary_key = False


class KanjiModel(BaseModel):
    """Kanji reading of an entry (table: Kanji)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="kanjis")
    text = TextField(null=True)

    class Meta:
        table_name = "Kanji"


class KJIModel(BaseModel):
    """Kanji info (table: KJI)."""

    kid = ForeignKeyField(KanjiModel, column_name="kid", backref="infos")
    text = TextField(null=True)

    class Meta:
        table_name = "KJI"
        primary_key = False


class KJPModel(BaseModel):
    """Kanji priority (table: KJP)."""

    kid = ForeignKeyField(KanjiModel, column_name="kid", backref="pris")
    text = TextField(null=True)

    class Meta:
        table_name = "KJP"
        primary_key = False


class KanaModel(BaseModel):
    """Kana reading of an entry (table: Kana)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="kanas")
    text = TextField(null=True)
    nokanji = BooleanField(null=True)

    class Meta:
        table_name = "Kana"


class KNIModel(BaseModel):
    """Kana info (table: KNI)."""

    kid = ForeignKeyField(KanaModel, column_name="kid", backref="infos")
    text = TextField(null=True)

    class Meta:
        table_name = "KNI"
        primary_key = False


class KNPModel(BaseModel):
    """Kana priority (table: KNP)."""

    kid = ForeignKeyField(KanaModel, column_name="kid", backref="pris")
    text = TextField(null=True)

    class Meta:
        table_name = "KNP"
        primary_key = False


class KNRModel(BaseModel):
    """Kana reading restriction (table: KNR)."""

    kid = ForeignKeyField(KanaModel, column_name="kid", backref="restrs")
    text = TextField(null=True)

    class Meta:
        table_name = "KNR"
        primary_key = False


class SenseModel(BaseModel):
    """Sense of an entry (table: Sense)."""

    idseq = ForeignKeyField(EntryModel, column_name="idseq", backref="senses")

    class Meta:
        table_name = "Sense"


class StagkModel(BaseModel):
    """Sense kanji restriction (table: stagk)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="stagks")
    text = TextField(null=True)

    class Meta:
        table_name = "stagk"
        primary_key = False


class StagrModel(BaseModel):
    """Sense kana restriction (table: stagr)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="stagrs")
    text = TextField(null=True)

    class Meta:
        table_name = "stagr"
        primary_key = False


class PosModel(BaseModel):
    """Part-of-speech tag (table: pos)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="poses")
    text = TextField(null=True)

    class Meta:
        table_name = "pos"
        primary_key = False


class XrefModel(BaseModel):
    """Cross-reference (table: xref)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="xrefs")
    text = TextField(null=True)

    class Meta:
        table_name = "xref"
        primary_key = False


class AntonymModel(BaseModel):
    """Antonym (table: antonym)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="antonyms")
    text = TextField(null=True)

    class Meta:
        table_name = "antonym"
        primary_key = False


class FieldModel(BaseModel):
    """Field of application (table: field)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="fields")
    text = TextField(null=True)

    class Meta:
        table_name = "field"
        primary_key = False


class MiscModel(BaseModel):
    """Miscellaneous info (table: misc)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="miscs")
    text = TextField(null=True)

    class Meta:
        table_name = "misc"
        primary_key = False


class SenseInfoModel(BaseModel):
    """Sense information note (table: SenseInfo)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="sense_infos")
    text = TextField(null=True)

    class Meta:
        table_name = "SenseInfo"
        primary_key = False


class SenseSourceModel(BaseModel):
    """Language source of a sense (table: SenseSource)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="sense_sources")
    text = TextField(null=True)
    lang = TextField(null=True)
    lstype = TextField(null=True)
    wasei = TextField(null=True)

    class Meta:
        table_name = "SenseSource"
        primary_key = False


class DialectModel(BaseModel):
    """Dialect tag (table: dialect)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="dialects")
    text = TextField(null=True)

    class Meta:
        table_name = "dialect"
        primary_key = False


class SenseGlossModel(BaseModel):
    """Gloss / translation (table: SenseGloss)."""

    sid = ForeignKeyField(SenseModel, column_name="sid", backref="glosses")
    lang = TextField(null=True)
    gend = TextField(null=True)
    text = TextField(null=True)

    class Meta:
        table_name = "SenseGloss"
        primary_key = False


# ---------------------------------------------------------------------------
# Phase 2.4 — ALL_MODELS
#
# Ordered so that parent tables are created before child tables (FK deps).
# Used by create_tables() / drop_tables().
# ---------------------------------------------------------------------------

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
# Phase 3 — JMDictSQLite class
# ---------------------------------------------------------------------------


class JMDictSQLite:
    """
    peewee-backed JMDict SQLite accessor.

    Public interface is identical to the puchikarui version in jmdict_sqlite.py
    so that util.py can swap implementations without changes elsewhere.

    The ``ctx`` parameter accepted by every public method is silently ignored —
    peewee manages connection reuse transparently via the module-level
    ``database`` object.
    """

    KEY_JMD_VER = "jmdict.version"
    KEY_JMD_URL = "jmdict.url"

    def __init__(self, db_path, *args, **kwargs):
        # Phase 3.1
        # Normalise the path: expand ~ and resolve to an absolute path so that
        # both makedirs and sqlite3.connect receive a clean, unambiguous path.
        # :memory: is left untouched.
        if db_path and db_path != ":memory:":
            db_path = os.path.abspath(os.path.expanduser(db_path))
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        database.init(
            db_path,
            pragmas={
                "foreign_keys": 0,  # off during bulk import for speed
            },
        )
        database.connect(reuse_if_open=True)
        database.create_tables(ALL_MODELS, safe=True)

        # Expose model classes as instance attributes so call sites like
        # ``self.db.Entry.select()`` and ``self.db.meta.select()`` continue
        # to work unchanged.
        self.Entry = EntryModel
        self.meta = MetaModel

        # Seed metadata rows the first time the DB is created (mirrors the
        # SETUP_SCRIPT in jmdict_sqlite.py).
        self._seed_meta()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _seed_meta(self):
        """Insert default metadata rows if they do not already exist."""
        defaults = [
            (self.KEY_JMD_VER, JMDICT_VERSION),
            (self.KEY_JMD_URL, JMDICT_URL),
            ("generator", "jamdict"),
            ("generator_version", JAMDICT_VERSION),
            ("generator_url", JAMDICT_URL),
        ]
        for key, value in defaults:
            MetaModel.get_or_create(key=key, defaults={"value": value})

    # ------------------------------------------------------------------
    # Phase 3.2 — update_jmd_meta
    # ------------------------------------------------------------------

    def update_jmd_meta(self, version, url, ctx=None):
        """Upsert jmdict.version and jmdict.url in the meta table."""
        MetaModel.insert(key=self.KEY_JMD_VER, value=version).on_conflict(
            conflict_target=[MetaModel.key],
            update={MetaModel.value: version},
        ).execute()
        MetaModel.insert(key=self.KEY_JMD_URL, value=url).on_conflict(
            conflict_target=[MetaModel.key],
            update={MetaModel.value: url},
        ).execute()

    # ------------------------------------------------------------------
    # Phase 3.3 — all_pos
    # ------------------------------------------------------------------

    def all_pos(self, ctx=None):
        """Return a sorted list of all distinct POS tags in the database."""
        return [row.text for row in PosModel.select(PosModel.text).distinct()]

    # ------------------------------------------------------------------
    # Phase 3.4 — _build_search_query
    # ------------------------------------------------------------------

    def _build_search_query(self, query, pos=None):
        """
        Mirror the search logic from jmdict_sqlite.py but expressed as peewee
        query expressions rather than raw SQL strings.

        Returns a SelectQuery of EntryModel rows matching the criteria.
        """
        q = EntryModel.select()

        if query.startswith("id#"):
            query_int = int(query[3:])
            if query_int >= 0:
                getLogger().debug("Searching by ID: {}".format(query_int))
                q = q.where(EntryModel.idseq == query_int)
        elif query and query != "%":
            _is_wildcard = "_" in query or "@" in query or "%" in query

            # Sub-queries for each search dimension
            kanji_sq = KanjiModel.select(KanjiModel.idseq).where(
                KanjiModel.text**query if _is_wildcard else KanjiModel.text == query
            )
            kana_sq = KanaModel.select(KanaModel.idseq).where(
                KanaModel.text**query if _is_wildcard else KanaModel.text == query
            )
            gloss_sq = (
                SenseModel.select(SenseModel.idseq)
                .join(SenseGlossModel, on=(SenseGlossModel.sid == SenseModel.id))
                .where(
                    SenseGlossModel.text**query
                    if _is_wildcard
                    else SenseGlossModel.text == query
                )
            )

            q = q.where(
                (EntryModel.idseq << kanji_sq)
                | (EntryModel.idseq << kana_sq)
                | (EntryModel.idseq << gloss_sq)
            )

        if pos:
            if isinstance(pos, str):
                getLogger().warning("POS filter should be a collection, not a string")
                pos = [pos]
            pos_sq = (
                SenseModel.select(SenseModel.idseq)
                .join(PosModel, on=(PosModel.sid == SenseModel.id))
                .where(PosModel.text.in_(pos))
            )
            q = q.where(EntryModel.idseq << pos_sq)

        getLogger().debug("Search query built for: %r  pos=%r", query, pos)
        return q

    # ------------------------------------------------------------------
    # Phase 3.5 — search
    # ------------------------------------------------------------------

    def search(self, query, ctx=None, pos=None, **kwargs):
        """Return a list of JMDEntry objects matching query."""
        return list(self.search_iter(query, ctx=ctx, pos=pos, **kwargs))

    # ------------------------------------------------------------------
    # Phase 3.6 — search_iter
    # ------------------------------------------------------------------

    def search_iter(self, query, ctx=None, pos=None, **kwargs):
        """Yield JMDEntry objects matching query one at a time."""
        for entry_row in self._build_search_query(query, pos=pos):
            yield self.get_entry(entry_row.idseq)

    # ------------------------------------------------------------------
    # Phase 3.7 — get_entry
    # ------------------------------------------------------------------

    def get_entry(self, idseq, ctx=None):
        """Reconstruct a full JMDEntry domain object from the database."""
        entry = JMDEntry(idseq)

        # ---- links / bibs / etym / audit --------------------------------
        dblinks = list(LinkModel.select().where(LinkModel.idseq == idseq))
        dbbibs = list(BibModel.select().where(BibModel.idseq == idseq))
        dbetym = list(EtymModel.select().where(EtymModel.idseq == idseq))
        dbaudit = list(AuditModel.select().where(AuditModel.idseq == idseq))

        if dblinks or dbbibs or dbetym or dbaudit:
            entry.info = EntryInfo()
            for lnk in dblinks:
                entry.info.links.append(Link(lnk.tag, lnk.desc, lnk.uri))
            for bib in dbbibs:
                entry.info.bibinfo.append(BibInfo(bib.tag, bib.text))
            for etym in dbetym:
                entry.info.etym.append(etym.text)
            for aud in dbaudit:
                entry.info.audit.append(Audit(aud.upd_date, aud.upd_detl))

        # ---- kanji forms ------------------------------------------------
        for dbkj in KanjiModel.select().where(KanjiModel.idseq == idseq):
            kj = KanjiForm(dbkj.text)
            for kji in KJIModel.select().where(KJIModel.kid == dbkj.id):
                kj.info.append(kji.text)
            for kjp in KJPModel.select().where(KJPModel.kid == dbkj.id):
                kj.pri.append(kjp.text)
            entry.kanji_forms.append(kj)

        # ---- kana forms -------------------------------------------------
        for dbkn in KanaModel.select().where(KanaModel.idseq == idseq):
            kn = KanaForm(dbkn.text, dbkn.nokanji)
            for kni in KNIModel.select().where(KNIModel.kid == dbkn.id):
                kn.info.append(kni.text)
            for knp in KNPModel.select().where(KNPModel.kid == dbkn.id):
                kn.pri.append(knp.text)
            for knr in KNRModel.select().where(KNRModel.kid == dbkn.id):
                kn.restr.append(knr.text)
            entry.kana_forms.append(kn)

        # ---- senses -----------------------------------------------------
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
    # Phase 3.8 — insert_entries
    # ------------------------------------------------------------------

    def insert_entries(self, entries, ctx=None):
        """Bulk-insert a collection of JMDEntry objects inside one transaction."""
        getLogger().debug("JMDict bulk insert {} entries".format(len(entries)))
        # Issue performance PRAGMAs *before* opening the transaction — SQLite
        # forbids changing journal_mode while a transaction is already active.
        # journal_mode=MEMORY is skipped for :memory: databases (no-op there).
        if database.database != ":memory:":
            database.execute_sql("PRAGMA journal_mode=MEMORY")
        database.execute_sql("PRAGMA cache_size=-65536")  # 64 MB
        database.execute_sql("PRAGMA temp_store=MEMORY")
        with database.atomic():
            for entry in entries:
                self.insert_entry(entry)

    # ------------------------------------------------------------------
    # Phase 3.9 — insert_entry
    # ------------------------------------------------------------------

    def insert_entry(self, entry, ctx=None):
        """Insert a single JMDEntry and all its related rows."""
        EntryModel.create(idseq=entry.idseq)

        # ---- entry info -------------------------------------------------
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

        # ---- kanji forms ------------------------------------------------
        for kj in entry.kanji_forms:
            dbkj = KanjiModel.create(idseq=entry.idseq, text=kj.text)
            for info in kj.info:
                KJIModel.create(kid=dbkj.id, text=info)
            for pri in kj.pri:
                KJPModel.create(kid=dbkj.id, text=pri)

        # ---- kana forms -------------------------------------------------
        for kn in entry.kana_forms:
            dbkn = KanaModel.create(idseq=entry.idseq, text=kn.text, nokanji=kn.nokanji)
            for info in kn.info:
                KNIModel.create(kid=dbkn.id, text=info)
            for pri in kn.pri:
                KNPModel.create(kid=dbkn.id, text=pri)
            for restr in kn.restr:
                KNRModel.create(kid=dbkn.id, text=restr)

        # ---- senses -----------------------------------------------------
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


# ---------------------------------------------------------------------------
# Phase 3.10 — alias used by util.py
# ---------------------------------------------------------------------------


class JamdictSQLite(JMDictSQLite):
    """Alias for JMDictSQLite — util.py instantiates this name."""

    pass
