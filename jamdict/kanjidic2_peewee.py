# -*- coding: utf-8 -*-

"""
KanjiDic2 SQLite backend — peewee implementation.

Each KanjiDic2DB instance owns its own SqliteDatabase object.  Model classes
are unbound at definition time (database=None) and bound to a specific
database instance via peewee's bind_ctx API inside every public method.  This
means multiple KanjiDic2DB instances with different paths — including :memory:
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
    CharField,
    ForeignKeyField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
)

from . import __url__ as JAMDICT_URL
from . import __version__ as JAMDICT_VERSION
from .kanjidic2 import (
    Character,
    CodePoint,
    DicRef,
    KanjiDic2,
    Kanjidic2XMLParser,
    Meaning,
    QueryCode,
    Radical,
    Reading,
    RMGroup,
    Variant,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KANJIDIC2_VERSION = "1.6"
KANJIDIC2_URL = "https://www.edrdg.org/wiki/index.php/KANJIDIC_Project"
KANJIDIC2_DATE = "April 2008"

KEY_FILE_VER = "kanjidic2.file_version"
KEY_DB_VER = "kanjidic2.database_version"
KEY_CREATED_DATE = "kanjidic2.date_of_creation"


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Model definitions — database=None (unbound)
#
# Models are defined once at module level with no database attached.
# KanjiDic2DB.__init__ uses bind_ctx so each instance gets its own
# connection without interfering with any other instance.
# ---------------------------------------------------------------------------


class _Base(Model):
    pass


class MetaModel(_Base):
    key = CharField(primary_key=True)
    value = TextField()

    class Meta:
        table_name = "meta"


class CharacterModel(_Base):
    ID = AutoField()
    literal = TextField()
    stroke_count = IntegerField(null=True)
    grade = TextField(null=True)
    freq = TextField(null=True)
    jlpt = TextField(null=True)

    class Meta:
        table_name = "character"


class CodePointModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="codepoints")
    cp_type = TextField(null=True)
    value = TextField(null=True)

    class Meta:
        table_name = "codepoint"
        primary_key = False


class RadicalModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="radicals")
    rad_type = TextField(null=True)
    value = TextField(null=True)

    class Meta:
        table_name = "radical"
        primary_key = False


class StrokeMiscountModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="stroke_miscounts")
    value = IntegerField(null=True)

    class Meta:
        table_name = "stroke_miscount"
        primary_key = False


class VariantModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="variants")
    var_type = TextField(null=True)
    value = TextField(null=True)

    class Meta:
        table_name = "variant"
        primary_key = False


class RadNameModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="rad_names")
    value = TextField(null=True)

    class Meta:
        table_name = "rad_name"
        primary_key = False


class DicRefModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="dic_refs")
    dr_type = TextField(null=True)
    value = TextField(null=True)
    m_vol = TextField(null=True)
    m_page = TextField(null=True)

    class Meta:
        table_name = "dic_ref"
        primary_key = False


class QueryCodeModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="query_codes")
    qc_type = TextField(null=True)
    value = TextField(null=True)
    skip_misclass = TextField(null=True)

    class Meta:
        table_name = "query_code"
        primary_key = False


class NanoriModel(_Base):
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="nanoris")
    value = TextField(null=True)

    class Meta:
        table_name = "nanori"
        primary_key = False


class RMGroupModel(_Base):
    ID = AutoField()
    cid = ForeignKeyField(CharacterModel, column_name="cid", backref="rm_groups")

    class Meta:
        table_name = "rm_group"


class ReadingModel(_Base):
    gid = ForeignKeyField(RMGroupModel, column_name="gid", backref="readings")
    r_type = TextField(null=True)
    value = TextField(null=True)
    on_type = TextField(null=True)
    r_status = TextField(null=True)

    class Meta:
        table_name = "reading"
        primary_key = False


class MeaningModel(_Base):
    gid = ForeignKeyField(RMGroupModel, column_name="gid", backref="meanings")
    value = TextField(null=True)
    m_lang = TextField(null=True)

    class Meta:
        table_name = "meaning"
        primary_key = False


# Ordered so parent tables are created before child tables.
ALL_MODELS = [
    MetaModel,
    CharacterModel,
    CodePointModel,
    RadicalModel,
    StrokeMiscountModel,
    VariantModel,
    RadNameModel,
    DicRefModel,
    QueryCodeModel,
    NanoriModel,
    RMGroupModel,
    ReadingModel,
    MeaningModel,
]


# ---------------------------------------------------------------------------
# KanjiDic2DB — the clean public API
# ---------------------------------------------------------------------------


class KanjiDic2DB:
    """
    peewee-backed KanjiDic2 SQLite store.

    Each instance owns its own SqliteDatabase connection.  Multiple instances
    with different paths (including ':memory:') can coexist in the same
    process.

    Typical usage::

        db = KanjiDic2DB("path/to/kanjidic2.db")

        # Import from parsed XML
        db.insert_chars(kd2.characters)

        # Query
        char = db.get_char("持")
        char = db.get_char_by_id(42)

        # Iterate
        for char in db.search_chars_iter(["持", "食", "飲"]):
            print(char)

        # Metadata
        db.update_kd2_meta(file_version, database_version, date_of_creation)
        db.get_meta("kanjidic2.file_version")   # → str | None
    """

    def __init__(self, db_path: str):
        """
        Open (or create) a KanjiDic2 SQLite database at *db_path*.

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
            (KEY_FILE_VER, ""),
            (KEY_DB_VER, ""),
            (KEY_CREATED_DATE, ""),
            ("kanjidic2.version", KANJIDIC2_VERSION),
            ("kanjidic2.url", KANJIDIC2_URL),
            ("kanjidic2.date", KANJIDIC2_DATE),
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

    def update_kd2_meta(
        self,
        file_version: str,
        database_version: str,
        date_of_creation: str,
    ) -> None:
        """Upsert the KanjiDic2 header metadata (file_version, database_version, date_of_creation)."""
        rows = [
            (KEY_FILE_VER, file_version),
            (KEY_DB_VER, database_version),
            (KEY_CREATED_DATE, date_of_creation),
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
    # Query
    # ------------------------------------------------------------------

    def get_char(self, literal: str) -> Optional[Character]:
        """Return the Character for the given *literal*, or None if not found."""
        with self._db.bind_ctx(ALL_MODELS):
            row = CharacterModel.get_or_none(CharacterModel.literal == literal)
            if row is None:
                return None
            return self._build_char(row)

    def get_char_by_id(self, cid: int) -> Optional[Character]:
        """Return the Character with the given internal *cid*, or None if not found."""
        with self._db.bind_ctx(ALL_MODELS):
            row = CharacterModel.get_or_none(CharacterModel.ID == cid)
            if row is None:
                return None
            return self._build_char(row)

    def _build_char(self, row: CharacterModel) -> Character:
        """
        Reconstruct a full Character domain object from a CharacterModel row.

        Must be called inside an active bind_ctx block.
        """
        c = Character()
        c.ID = row.ID
        c.literal = row.literal
        c.stroke_count = row.stroke_count
        c.grade = row.grade
        c.freq = row.freq
        c.jlpt = row.jlpt

        # codepoints
        for cp_row in CodePointModel.select().where(CodePointModel.cid == row.ID):
            cp = CodePoint(cp_row.cp_type or "", cp_row.value or "")
            cp.cid = row.ID
            c.codepoints.append(cp)

        # radicals
        for rad_row in RadicalModel.select().where(RadicalModel.cid == row.ID):
            rad = Radical(rad_row.rad_type or "", rad_row.value or "")
            rad.cid = row.ID
            c.radicals.append(rad)

        # stroke miscounts
        for smc_row in StrokeMiscountModel.select().where(
            StrokeMiscountModel.cid == row.ID
        ):
            c.stroke_miscounts.append(smc_row.value)

        # variants
        for v_row in VariantModel.select().where(VariantModel.cid == row.ID):
            v = Variant(v_row.var_type or "", v_row.value or "")
            v.cid = row.ID
            c.variants.append(v)

        # rad_names
        for rn_row in RadNameModel.select().where(RadNameModel.cid == row.ID):
            c.rad_names.append(rn_row.value)

        # dic_refs
        for dr_row in DicRefModel.select().where(DicRefModel.cid == row.ID):
            dr = DicRef(
                dr_row.dr_type or "",
                dr_row.value or "",
                dr_row.m_vol or "",
                dr_row.m_page or "",
            )
            dr.cid = row.ID
            c.dic_refs.append(dr)

        # query_codes
        for qc_row in QueryCodeModel.select().where(QueryCodeModel.cid == row.ID):
            qc = QueryCode(
                qc_row.qc_type or "", qc_row.value or "", qc_row.skip_misclass or ""
            )
            qc.cid = row.ID
            c.query_codes.append(qc)

        # nanoris
        for n_row in NanoriModel.select().where(NanoriModel.cid == row.ID):
            c.nanoris.append(n_row.value)

        # rm_groups
        for rmg_row in RMGroupModel.select().where(RMGroupModel.cid == row.ID):
            rmg = RMGroup()
            rmg.ID = rmg_row.ID
            rmg.cid = row.ID
            for r_row in ReadingModel.select().where(ReadingModel.gid == rmg_row.ID):
                r = Reading(
                    r_row.r_type or "",
                    r_row.value or "",
                    r_row.on_type or "",
                    r_row.r_status or "",
                )
                r.gid = rmg_row.ID
                rmg.readings.append(r)
            for m_row in MeaningModel.select().where(MeaningModel.gid == rmg_row.ID):
                m = Meaning(m_row.value or "", m_row.m_lang or "")
                m.gid = rmg_row.ID
                rmg.meanings.append(m)
            c.rm_groups.append(rmg)

        return c

    def search_chars_iter(self, literals) -> Iterator[Character]:
        """
        Yield a Character for each literal in *literals* that exists in the database.

        Skips literals that are not found rather than raising an error.
        """
        for literal in literals:
            c = self.get_char(literal)
            if c is not None:
                yield c

    def all_chars(self) -> List[Character]:
        """Return all characters in the database as a list."""
        with self._db.bind_ctx(ALL_MODELS):
            cids = [row.ID for row in CharacterModel.select(CharacterModel.ID)]
        result = []
        for cid in cids:
            c = self.get_char_by_id(cid)
            if c is not None:
                result.append(c)
        return result

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def insert_chars(self, chars) -> None:
        """
        Bulk-insert a collection of Character objects.

        Wraps the entire operation in a single transaction with performance
        PRAGMAs to match the throughput of the original puchikarui buckmode.
        """
        getLogger().debug("KanjiDic2DB: bulk insert %d characters", len(chars))
        with self._db.bind_ctx(ALL_MODELS):
            if self._db_path != ":memory:":
                self._db.execute_sql("PRAGMA journal_mode=MEMORY")
            self._db.execute_sql("PRAGMA cache_size=-65536")
            self._db.execute_sql("PRAGMA temp_store=MEMORY")
            with self._db.atomic():
                for c in chars:
                    self._insert_char_unsafe(c)

    def insert_char(self, c: Character) -> None:
        """Insert a single Character and all its child rows."""
        with self._db.bind_ctx(ALL_MODELS):
            self._insert_char_unsafe(c)

    def _insert_char_unsafe(self, c: Character) -> None:
        """
        Insert a single Character without acquiring bind_ctx.

        Must only be called from within an active bind_ctx block.
        """
        row = CharacterModel.create(
            literal=c.literal,
            stroke_count=c.stroke_count,
            grade=c.grade,
            freq=c.freq,
            jlpt=c.jlpt,
        )
        # propagate the DB-assigned ID back to the domain object so that
        # callers (e.g. test_xml2sqlite) can use c.ID after insertion
        c.ID = row.ID

        # codepoints
        for cp in c.codepoints:
            CodePointModel.create(cid=row.ID, cp_type=cp.cp_type, value=cp.value)

        # radicals
        for rad in c.radicals:
            RadicalModel.create(cid=row.ID, rad_type=rad.rad_type, value=rad.value)

        # stroke miscounts
        for smc in c.stroke_miscounts:
            StrokeMiscountModel.create(cid=row.ID, value=smc)

        # variants
        for v in c.variants:
            VariantModel.create(cid=row.ID, var_type=v.var_type, value=v.value)

        # rad_names
        for rn in c.rad_names:
            RadNameModel.create(cid=row.ID, value=rn)

        # dic_refs
        for dr in c.dic_refs:
            DicRefModel.create(
                cid=row.ID,
                dr_type=dr.dr_type,
                value=dr.value,
                m_vol=dr.m_vol,
                m_page=dr.m_page,
            )

        # query_codes
        for qc in c.query_codes:
            QueryCodeModel.create(
                cid=row.ID,
                qc_type=qc.qc_type,
                value=qc.value,
                skip_misclass=qc.skip_misclass,
            )

        # nanoris
        for n in c.nanoris:
            NanoriModel.create(cid=row.ID, value=n)

        # rm_groups
        for rmg in c.rm_groups:
            rmg_row = RMGroupModel.create(cid=row.ID)
            rmg.ID = rmg_row.ID
            for r in rmg.readings:
                ReadingModel.create(
                    gid=rmg_row.ID,
                    r_type=r.r_type,
                    value=r.value,
                    on_type=r.on_type,
                    r_status=r.r_status,
                )
            for m in rmg.meanings:
                MeaningModel.create(gid=rmg_row.ID, value=m.value, m_lang=m.m_lang)

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
        return f"KanjiDic2DB({self._db_path!r})"
