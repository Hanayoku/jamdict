#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pytest test suite for the peewee-backed JMDict SQLite implementation.

Mirrors the assertions in test_jmdict_sqlite.py but is written with pytest
fixtures instead of unittest.TestCase so the two suites can be compared
directly.  All expected counts and field values are derived from
test/data/JMdict_mini.xml (230 entries).
"""

# This code is a part of jamdict library: https://github.com/neocl/jamdict
# :copyright: (c) 2016 Le Tuan Anh <tuananh.ke@gmail.com>
# :license: MIT, see LICENSE for more details.

import os
from pathlib import Path

import pytest

from jamdict import JMDictXML
from jamdict.jmdict_peewee import (
    JamdictSQLite,
    JMDictSQLite,
    MetaModel,
    database,
)

# ---------------------------------------------------------------------------
# Helper: reconnect the module-level database singleton to a given path.
# Because peewee uses a single module-level Database object, any call to
# database.init() re-points ALL models at the new path.  Tests that use
# ram_db or empty_db overwrite the singleton; tests that subsequently want
# the file-backed DB must re-init it before executing.
# ---------------------------------------------------------------------------


def _reconnect(path: str) -> None:
    """Re-initialise and reconnect the peewee database singleton."""
    if not database.is_closed():
        database.close()
    database.init(path, pragmas={"foreign_keys": 0})
    database.connect(reuse_if_open=True)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TEST_DIR = Path(os.path.realpath(__file__)).parent
TEST_DATA = TEST_DIR / "data"
TEST_DB = TEST_DATA / "test_peewee.db"
MINI_JMD = TEST_DATA / "JMdict_mini.xml"


# ---------------------------------------------------------------------------
# Session-scoped XML source (parsed once for the whole test run)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def xml_entries():
    """Parse JMdict_mini.xml once and return the JMDictXML object."""
    return JMDictXML.from_file(str(MINI_JMD))


# ---------------------------------------------------------------------------
# File-backed DB fixture (one per test module run)
#
# The DB file is removed before the suite starts so every test run gets a
# clean slate, matching the setUpClass behaviour in the unittest version.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def file_db(xml_entries):
    """
    A file-backed JMDictSQLite populated with all entries from the mini XML.

    Scoped to the module so the (relatively expensive) bulk import runs once
    and all tests in this file share the same populated database.
    """
    TEST_DATA.mkdir(exist_ok=True)
    if TEST_DB.exists():
        TEST_DB.unlink()

    db = JMDictSQLite(str(TEST_DB))
    db.insert_entries(xml_entries)
    yield db

    # Teardown: close the peewee connection and remove the file.
    if not database.is_closed():
        database.close()
    if TEST_DB.exists():
        TEST_DB.unlink()


# ---------------------------------------------------------------------------
# In-memory DB fixture (fresh per test function)
#
# Each test that needs a RAM database gets its own empty instance, imports
# data itself, and leaves no side effects.
# ---------------------------------------------------------------------------


@pytest.fixture()
def ram_db(xml_entries):
    """
    A fresh :memory: JMDictSQLite, pre-populated with the mini XML entries.

    Scoped to each test function so tests are fully isolated.
    """
    db = JMDictSQLite(":memory:")
    db.insert_entries(xml_entries)
    yield db

    if not database.is_closed():
        database.close()


# ---------------------------------------------------------------------------
# Empty in-memory DB (no entries imported) for insert-focused tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def empty_db():
    """A fresh, empty :memory: JMDictSQLite with no entries."""
    db = JMDictSQLite(":memory:")
    yield db

    if not database.is_closed():
        database.close()


# ===========================================================================
# 1.  Import tests
# ===========================================================================


class TestImport:
    """Verify that XML entries can be written to both file and RAM databases."""

    def test_xml2sqlite_entry_count(self, file_db, xml_entries):
        """Number of rows in Entry table must equal number of XML entries."""
        db_count = len(list(file_db.Entry.select()))
        xml_count = len(xml_entries)
        assert db_count == xml_count, (
            f"DB has {db_count} entries but XML has {xml_count}"
        )

    def test_xml2sqlite_okashi_kanji(self, file_db):
        """Entry 1001710 (お菓子) must round-trip with the correct first kanji."""
        e = file_db.get_entry("1001710")
        assert e is not None
        d = e.to_dict()
        assert d["kanji"][0]["text"] == "お菓子"

    def test_xml2sqlite_okashi_kana(self, file_db):
        """Entry 1001710 (お菓子) must have kana form おかし."""
        e = file_db.get_entry("1001710")
        d = e.to_dict()
        assert d["kana"][0]["text"] == "おかし"

    def test_xml2sqlite_okashi_gloss(self, file_db):
        """Entry 1001710 (お菓子) must contain the gloss 'confections'."""
        e = file_db.get_entry("1001710")
        all_glosses = [
            g["text"] for s in e.to_dict()["senses"] for g in s["SenseGloss"]
        ]
        assert "confections" in all_glosses

    def test_import_to_ram_entry_count(self, ram_db, xml_entries):
        """After bulk import into :memory:, entry count must match XML."""
        db_count = len(list(ram_db.Entry.select()))
        assert db_count == len(xml_entries)

    def test_insert_entries_idempotent_count(self, empty_db, xml_entries):
        """insert_entries writes the expected number of rows."""
        empty_db.insert_entries(xml_entries)
        assert len(list(empty_db.Entry.select())) == len(xml_entries)

    def test_insert_single_entry(self, empty_db, xml_entries):
        """insert_entry for one entry then get_entry returns the same data."""
        src = next(x for x in xml_entries if x.idseq == "1001710")
        empty_db.insert_entry(src)
        retrieved = empty_db.get_entry("1001710")
        assert retrieved.idseq == "1001710"
        assert retrieved.kanji_forms[0].text == "お菓子"
        assert retrieved.kana_forms[0].text == "おかし"


# ===========================================================================
# 2.  get_entry tests
# ===========================================================================


class TestGetEntry:
    """Verify deep reconstruction of JMDEntry domain objects."""

    def test_get_entry_returns_correct_idseq(self, ram_db):
        e = ram_db.get_entry("1001710")
        assert str(e.idseq) == "1001710"

    def test_get_entry_multiple_kanji_forms(self, ram_db):
        """お菓子 has two kanji forms: お菓子 and 御菓子."""
        e = ram_db.get_entry("1001710")
        kanji_texts = [k.text for k in e.kanji_forms]
        assert "お菓子" in kanji_texts
        assert "御菓子" in kanji_texts

    def test_get_entry_kana_forms(self, ram_db):
        """お菓子 has exactly one kana form: おかし."""
        e = ram_db.get_entry("1001710")
        assert len(e.kana_forms) == 1
        assert e.kana_forms[0].text == "おかし"

    def test_get_entry_senses_not_empty(self, ram_db):
        e = ram_db.get_entry("1001710")
        assert len(e.senses) >= 1

    def test_get_entry_gloss_texts(self, ram_db):
        """All three glosses for お菓子 must be present."""
        e = ram_db.get_entry("1001710")
        glosses = {g.text for s in e.senses for g in s.gloss}
        assert {"confections", "sweets", "candy"}.issubset(glosses)

    def test_get_entry_pos_tag(self, ram_db):
        """お菓子 should be tagged as a common noun."""
        e = ram_db.get_entry("1001710")
        all_pos = [p for s in e.senses for p in s.pos]
        assert any("noun" in p for p in all_pos)

    def test_get_entry_no_kanji_entry(self, ram_db):
        """Entry 1000000 has no kanji forms — only kana."""
        e = ram_db.get_entry("1000000")
        assert len(e.kanji_forms) == 0
        assert len(e.kana_forms) >= 1

    def test_get_entry_multiple_kana_forms(self, ram_db):
        """Entry 1000060 (々) has five kana forms."""
        e = ram_db.get_entry("1000060")
        assert len(e.kana_forms) >= 2

    def test_get_entry_multiple_senses(self, ram_db):
        """Entry 1000320 (あそこ) has multiple senses."""
        e = ram_db.get_entry("1000320")
        assert len(e.senses) >= 2

    def test_get_entry_lsource(self, ram_db):
        """Entry 1002480 (お転婆) has a Dutch language source."""
        e = ram_db.get_entry("1002480")
        all_sources = [ls for s in e.senses for ls in s.lsource]
        assert any(ls.lang == "dut" for ls in all_sources)
        assert any("ontembaar" in (ls.text or "") for ls in all_sources)

    def test_get_entry_to_dict_structure(self, ram_db):
        """to_dict() output must contain the expected top-level keys."""
        e = ram_db.get_entry("1001710")
        d = e.to_dict()
        assert "kanji" in d
        assert "kana" in d
        assert "senses" in d


# ===========================================================================
# 3.  search tests
# ===========================================================================


class TestSearch:
    """Verify search() returns correct result sets."""

    def test_search_exact_kana(self, ram_db):
        """Exact kana search for あの returns exactly 2 entries."""
        results = ram_db.search("あの")
        assert len(results) == 2

    def test_search_exact_kana_idseqs(self, ram_db):
        """The two あの entries must be 1000420 and 1000430."""
        results = ram_db.search("あの")
        ids = {str(e.idseq) for e in results}
        assert ids == {"1000420", "1000430"}

    def test_search_wildcard_kanji(self, ram_db):
        """Wildcard %子% in kanji returns exactly 4 entries."""
        results = ram_db.search("%子%")
        assert len(results) == 4

    def test_search_wildcard_kanji_contains_okashi(self, ram_db):
        """The %子% results must include お菓子 (1001710)."""
        results = ram_db.search("%子%")
        ids = {str(e.idseq) for e in results}
        assert "1001710" in ids

    def test_search_wildcard_gloss(self, ram_db):
        """Wildcard gloss search for %confections% returns at least one entry."""
        results = ram_db.search("%confections%")
        assert len(results) >= 1

    def test_search_wildcard_gloss_contains_okashi(self, ram_db):
        """The %confections% result must include 1001710."""
        results = ram_db.search("%confections%")
        ids = {str(e.idseq) for e in results}
        assert "1001710" in ids

    def test_search_wildcard_kana(self, ram_db):
        """Wildcard %あの% in kana matches 4 entries."""
        results = ram_db.search("%あの%")
        assert len(results) == 4

    def test_search_by_id(self, ram_db):
        """id# prefix search retrieves the entry by its sequence number."""
        results = ram_db.search("id#1001710")
        assert len(results) == 1
        assert str(results[0].idseq) == "1001710"

    def test_search_no_results(self, ram_db):
        """A query that matches nothing returns an empty list."""
        results = ram_db.search("zzznomatch999")
        assert results == []

    def test_search_returns_list(self, ram_db):
        """search() must always return a list, not a generator."""
        results = ram_db.search("あの")
        assert isinstance(results, list)

    def test_search_exact_kanji(self, ram_db):
        """Exact kanji search for お菓子 returns exactly 1 entry."""
        results = ram_db.search("お菓子")
        assert len(results) == 1
        assert str(results[0].idseq) == "1001710"

    def test_search_exact_gloss(self, ram_db):
        """Exact gloss search for 'confections' returns 1 entry."""
        results = ram_db.search("confections")
        assert len(results) == 1

    def test_search_ctx_kwarg_accepted(self, ram_db):
        """Passing ctx=None explicitly must not raise."""
        results = ram_db.search("あの", ctx=None)
        assert len(results) == 2

    def test_search_pos_filter(self, ram_db):
        """Passing a pos filter narrows results to matching POS tags."""
        all_results = ram_db.search("%あの%")
        pos_filtered = ram_db.search("%あの%", pos=["pronoun"])
        # pronoun-filtered subset must be smaller or equal
        assert len(pos_filtered) <= len(all_results)
        # every returned entry must actually carry the requested POS
        for entry in pos_filtered:
            all_pos = [p for s in entry.senses for p in s.pos]
            assert any("pronoun" in p for p in all_pos)


# ===========================================================================
# 4.  search_iter tests
# ===========================================================================


class TestSearchIter:
    """Verify search_iter() yields the same entries as search()."""

    def test_search_iter_yields_same_count_as_search(self, ram_db):
        iter_results = list(ram_db.search_iter("あの"))
        list_results = ram_db.search("あの")
        assert len(iter_results) == len(list_results)

    def test_search_iter_wildcard_kana_forms(self, ram_db):
        """Iterating %あの% must surface the expected superset of kana forms."""
        forms = set()
        for entry in ram_db.search_iter("%あの%"):
            forms.update(f.text for f in entry.kana_forms)
        expected = {"あのー", "あのう", "あの", "かの", "あのかた", "あのひと"}
        assert expected.issubset(forms)

    def test_search_iter_returns_jmdentry_objects(self, ram_db):
        """Each yielded item must be a JMDEntry."""
        from jamdict.jmdict import JMDEntry

        for entry in ram_db.search_iter("あの"):
            assert isinstance(entry, JMDEntry)

    def test_search_iter_no_results(self, ram_db):
        """An iterator over a non-matching query yields nothing."""
        results = list(ram_db.search_iter("zzznomatch999"))
        assert results == []

    def test_search_iter_ctx_kwarg_accepted(self, ram_db):
        """Passing ctx=None explicitly must not raise."""
        results = list(ram_db.search_iter("あの", ctx=None))
        assert len(results) == 2


# ===========================================================================
# 5.  all_pos tests
# ===========================================================================


class TestAllPos:
    """Verify all_pos() returns the complete set of distinct POS tags."""

    def test_all_pos_returns_list(self, ram_db):
        assert isinstance(ram_db.all_pos(), list)

    def test_all_pos_not_empty(self, ram_db):
        assert len(ram_db.all_pos()) > 0

    def test_all_pos_count(self, ram_db):
        """Mini XML has 22 distinct POS tags."""
        assert len(ram_db.all_pos()) == 22

    def test_all_pos_contains_noun(self, ram_db):
        pos = ram_db.all_pos()
        assert any("noun (common)" in p for p in pos)

    def test_all_pos_no_duplicates(self, ram_db):
        pos = ram_db.all_pos()
        assert len(pos) == len(set(pos))

    def test_all_pos_ctx_kwarg_accepted(self, ram_db):
        """Passing ctx=None explicitly must not raise."""
        pos = ram_db.all_pos(ctx=None)
        assert len(pos) > 0


# ===========================================================================
# 6.  update_jmd_meta tests
# ===========================================================================


class TestUpdateMeta:
    """Verify update_jmd_meta() upserts version and URL rows correctly."""

    def test_update_jmd_meta_sets_version(self, empty_db):
        empty_db.update_jmd_meta("9.99", "http://example.com/jmdict")
        row = MetaModel.get(MetaModel.key == "jmdict.version")
        assert row.value == "9.99"

    def test_update_jmd_meta_sets_url(self, empty_db):
        empty_db.update_jmd_meta("9.99", "http://example.com/jmdict")
        row = MetaModel.get(MetaModel.key == "jmdict.url")
        assert row.value == "http://example.com/jmdict"

    def test_update_jmd_meta_overwrites_version(self, empty_db):
        """Calling update_jmd_meta twice must update, not duplicate."""
        empty_db.update_jmd_meta("1.0", "http://a.com")
        empty_db.update_jmd_meta("2.0", "http://b.com")
        rows = list(MetaModel.select().where(MetaModel.key == "jmdict.version"))
        assert len(rows) == 1
        assert rows[0].value == "2.0"

    def test_update_jmd_meta_ctx_kwarg_accepted(self, empty_db):
        """Passing ctx=None explicitly must not raise."""
        empty_db.update_jmd_meta("1.0", "http://example.com", ctx=None)
        row = MetaModel.get(MetaModel.key == "jmdict.version")
        assert row.value == "1.0"


# ===========================================================================
# 7.  Model attribute exposure tests  (self.db.Entry, self.db.meta)
# ===========================================================================


class TestModelAttributes:
    """Verify that Entry and meta are exposed as direct instance attributes.

    Each test method receives ``file_db`` and also triggers the
    ``_repoint_to_file_db`` autouse fixture which re-initialises the
    module-level database singleton back to the file path.  This is
    necessary because per-function ``ram_db`` / ``empty_db`` fixtures used
    by earlier test classes call ``database.init(':memory:')`` and thereby
    disconnect the singleton from the file.
    """

    @pytest.fixture(autouse=True)
    def _repoint_to_file_db(self, file_db):
        """Re-connect the database singleton to the file DB before each test."""
        _reconnect(str(TEST_DB))

    def test_entry_attribute_is_callable(self, file_db):
        assert hasattr(file_db, "Entry")
        assert callable(getattr(file_db.Entry, "select", None))

    def test_entry_select_returns_all_rows(self, file_db, xml_entries):
        rows = list(file_db.Entry.select())
        assert len(rows) == len(xml_entries)

    def test_meta_attribute_is_callable(self, file_db):
        assert hasattr(file_db, "meta")
        assert callable(getattr(file_db.meta, "select", None))

    def test_meta_select_contains_version_key(self, file_db):
        keys = {row.key for row in file_db.meta.select()}
        assert "jmdict.version" in keys

    def test_meta_select_contains_url_key(self, file_db):
        keys = {row.key for row in file_db.meta.select()}
        assert "jmdict.url" in keys

    def test_meta_select_contains_generator_key(self, file_db):
        keys = {row.key for row in file_db.meta.select()}
        assert "generator" in keys


# ===========================================================================
# 8.  JamdictSQLite alias test
# ===========================================================================


class TestJamdictAlias:
    """JamdictSQLite must be a transparent subclass of JMDictSQLite."""

    def test_alias_is_subclass(self):
        assert issubclass(JamdictSQLite, JMDictSQLite)

    def test_alias_instantiates_with_memory_path(self):
        db = JamdictSQLite(":memory:")
        assert hasattr(db, "Entry")
        assert hasattr(db, "meta")

    def test_alias_search_works(self, xml_entries):
        db = JamdictSQLite(":memory:")
        db.insert_entries(xml_entries)
        results = db.search("あの")
        assert len(results) == 2


# ===========================================================================
# 9.  End-to-end import via Jamdict public API
# ===========================================================================


class TestJamdictAPIImport:
    """Verify the high-level Jamdict.import_data() path works with the peewee backend."""

    def test_import_data_via_jamdict(self):
        """
        Jamdict(db_file=':memory:', jmd_xml_file=...) should import without error.

        This exercises the util.py code path that calls insert_entries and
        insert_entry — the most important integration smoke-test.
        """
        from jamdict import Jamdict

        jd = Jamdict(
            db_file=":memory:",
            jmd_xml_file=str(MINI_JMD),
            auto_config=False,
            auto_expand=False,
        )
        # Should not raise
        jd.import_data()
