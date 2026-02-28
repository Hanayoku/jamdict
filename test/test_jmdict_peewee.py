#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pytest test suite for the peewee-backed JMDict implementation.

Tests the clean JMDictDB and JamdictPeewee APIs defined in:
  - jamdict/jmdict_peewee.py  (JMDictDB)
  - jamdict/jamdict_peewee.py (JamdictPeewee)

Test data: test/data/JMdict_mini.xml (230 entries)
"""

import os
from pathlib import Path

import pytest

from jamdict.jamdict_peewee import JamdictPeewee, LookupResult
from jamdict.jmdict import JMDEntry
from jamdict.jmdict_peewee import JMDictDB

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TEST_DIR = Path(os.path.realpath(__file__)).parent
TEST_DATA = TEST_DIR / "data"
TEST_DB = TEST_DATA / "test_jmdict_peewee.db"
MINI_JMD = TEST_DATA / "JMdict_mini.xml"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def xml_entries():
    """Parse JMdict_mini.xml once for the whole test session."""
    from jamdict.jmdict import JMDictXMLParser

    parser = JMDictXMLParser()
    return parser.parse_file(str(MINI_JMD))


@pytest.fixture(scope="module")
def file_db(xml_entries):
    """
    File-backed JMDictDB populated with all mini-XML entries.
    Created once per module; the DB file is wiped before population.
    """
    TEST_DATA.mkdir(exist_ok=True)
    if TEST_DB.exists():
        TEST_DB.unlink()

    db = JMDictDB(str(TEST_DB))
    db.insert_entries(xml_entries)
    yield db
    db.close()
    if TEST_DB.exists():
        TEST_DB.unlink()


@pytest.fixture()
def ram_db(xml_entries):
    """Fresh :memory: JMDictDB pre-populated with mini-XML entries."""
    db = JMDictDB(":memory:")
    db.insert_entries(xml_entries)
    yield db
    db.close()


@pytest.fixture()
def empty_db():
    """Fresh, empty :memory: JMDictDB with no entries imported."""
    db = JMDictDB(":memory:")
    yield db
    db.close()


@pytest.fixture()
def jam(tmp_path, xml_entries):
    """JamdictPeewee backed by a temp-dir DB, pre-populated."""
    db_path = str(tmp_path / "jmdict.db")
    runner = JamdictPeewee(db_path=db_path, xml_path=str(MINI_JMD))
    runner.import_data()
    yield runner
    runner.close()


# ===========================================================================
# 1. Import tests  (JMDictDB.insert_entries / insert_entry)
# ===========================================================================


class TestImport:
    def test_entry_count_matches_xml(self, file_db, xml_entries):
        """Row count in Entry table must equal the number of parsed XML entries."""
        from jamdict.jmdict_peewee import EntryModel

        # Re-bind EntryModel query to the file_db connection
        with file_db._db.bind_ctx([EntryModel]):
            count = EntryModel.select().count()
        assert count == len(xml_entries)

    def test_import_to_ram(self, ram_db, xml_entries):
        """In-memory import must also produce the correct entry count."""
        from jamdict.jmdict_peewee import EntryModel

        with ram_db._db.bind_ctx([EntryModel]):
            count = EntryModel.select().count()
        assert count == len(xml_entries)

    def test_insert_single_entry_roundtrip(self, empty_db, xml_entries):
        """insert_entry followed by get_entry must reproduce the original data."""
        src = next(e for e in xml_entries if str(e.idseq) == "1001710")
        empty_db.insert_entry(src)
        result = empty_db.get_entry(1001710)
        assert result is not None
        assert str(result.idseq) == "1001710"
        assert result.kanji_forms[0].text == "お菓子"
        assert result.kana_forms[0].text == "おかし"

    def test_insert_entries_count(self, empty_db, xml_entries):
        """insert_entries must write the expected number of rows."""
        from jamdict.jmdict_peewee import EntryModel

        empty_db.insert_entries(xml_entries)
        with empty_db._db.bind_ctx([EntryModel]):
            count = EntryModel.select().count()
        assert count == len(xml_entries)


# ===========================================================================
# 2. get_entry tests
# ===========================================================================


class TestGetEntry:
    def test_returns_correct_idseq(self, ram_db):
        e = ram_db.get_entry(1001710)
        assert str(e.idseq) == "1001710"

    def test_returns_none_for_missing(self, ram_db):
        assert ram_db.get_entry(9999999999) is None

    def test_okashi_kanji_forms(self, ram_db):
        """お菓子 has two kanji forms: お菓子 and 御菓子."""
        e = ram_db.get_entry(1001710)
        texts = [k.text for k in e.kanji_forms]
        assert "お菓子" in texts
        assert "御菓子" in texts

    def test_okashi_kana_forms(self, ram_db):
        """お菓子 has exactly one kana form: おかし."""
        e = ram_db.get_entry(1001710)
        assert len(e.kana_forms) == 1
        assert e.kana_forms[0].text == "おかし"

    def test_okashi_glosses(self, ram_db):
        """All three glosses for お菓子 must be present."""
        e = ram_db.get_entry(1001710)
        glosses = {g.text for s in e.senses for g in s.gloss}
        assert {"confections", "sweets", "candy"}.issubset(glosses)

    def test_okashi_pos_tag(self, ram_db):
        """お菓子 must carry a noun POS tag."""
        e = ram_db.get_entry(1001710)
        all_pos = [p for s in e.senses for p in s.pos]
        assert any("noun" in p for p in all_pos)

    def test_entry_with_no_kanji(self, ram_db):
        """Entry 1000000 is kana-only."""
        e = ram_db.get_entry(1000000)
        assert len(e.kanji_forms) == 0
        assert len(e.kana_forms) >= 1

    def test_entry_multiple_kana_forms(self, ram_db):
        """Entry 1000060 (々) has several kana forms."""
        e = ram_db.get_entry(1000060)
        assert len(e.kana_forms) >= 2

    def test_entry_multiple_senses(self, ram_db):
        """Entry 1000320 (あそこ) has more than one sense."""
        e = ram_db.get_entry(1000320)
        assert len(e.senses) >= 2

    def test_entry_lsource(self, ram_db):
        """Entry 1002480 (お転婆) has a Dutch language source."""
        e = ram_db.get_entry(1002480)
        all_sources = [ls for s in e.senses for ls in s.lsource]
        assert any(ls.lang == "dut" for ls in all_sources)
        assert any("ontembaar" in (ls.text or "") for ls in all_sources)

    def test_to_dict_structure(self, ram_db):
        e = ram_db.get_entry(1001710)
        d = e.to_dict()
        assert "kanji" in d
        assert "kana" in d
        assert "senses" in d

    def test_returns_jmdentry_instance(self, ram_db):
        e = ram_db.get_entry(1001710)
        assert isinstance(e, JMDEntry)


# ===========================================================================
# 3. search tests
# ===========================================================================


class TestSearch:
    def test_exact_kana_count(self, ram_db):
        assert len(ram_db.search("あの")) == 2

    def test_exact_kana_idseqs(self, ram_db):
        ids = {str(e.idseq) for e in ram_db.search("あの")}
        assert ids == {"1000420", "1000430"}

    def test_wildcard_kanji_count(self, ram_db):
        assert len(ram_db.search("%子%")) == 4

    def test_wildcard_kanji_includes_okashi(self, ram_db):
        ids = {str(e.idseq) for e in ram_db.search("%子%")}
        assert "1001710" in ids

    def test_wildcard_gloss(self, ram_db):
        results = ram_db.search("%confections%")
        assert len(results) >= 1

    def test_wildcard_gloss_includes_okashi(self, ram_db):
        ids = {str(e.idseq) for e in ram_db.search("%confections%")}
        assert "1001710" in ids

    def test_wildcard_kana_count(self, ram_db):
        assert len(ram_db.search("%あの%")) == 4

    def test_search_by_id_prefix(self, ram_db):
        results = ram_db.search("id#1001710")
        assert len(results) == 1
        assert str(results[0].idseq) == "1001710"

    def test_no_results(self, ram_db):
        assert ram_db.search("zzznomatch999") == []

    def test_returns_list(self, ram_db):
        assert isinstance(ram_db.search("あの"), list)

    def test_exact_kanji(self, ram_db):
        results = ram_db.search("お菓子")
        assert len(results) == 1
        assert str(results[0].idseq) == "1001710"

    def test_exact_gloss(self, ram_db):
        assert len(ram_db.search("confections")) == 1

    def test_pos_filter_narrows_results(self, ram_db):
        all_r = ram_db.search("%あの%")
        filtered = ram_db.search("%あの%", pos=["pronoun"])
        assert len(filtered) <= len(all_r)

    def test_pos_filter_entries_carry_pos(self, ram_db):
        for entry in ram_db.search("%あの%", pos=["pronoun"]):
            all_pos = [p for s in entry.senses for p in s.pos]
            assert any("pronoun" in p for p in all_pos)


# ===========================================================================
# 4. search_iter tests
# ===========================================================================


class TestSearchIter:
    def test_yields_same_count_as_search(self, ram_db):
        assert len(list(ram_db.search_iter("あの"))) == len(ram_db.search("あの"))

    def test_wildcard_kana_forms(self, ram_db):
        forms = set()
        for entry in ram_db.search_iter("%あの%"):
            forms.update(f.text for f in entry.kana_forms)
        expected = {"あのー", "あのう", "あの", "かの", "あのかた", "あのひと"}
        assert expected.issubset(forms)

    def test_yields_jmdentry_objects(self, ram_db):
        for entry in ram_db.search_iter("あの"):
            assert isinstance(entry, JMDEntry)

    def test_no_results(self, ram_db):
        assert list(ram_db.search_iter("zzznomatch999")) == []


# ===========================================================================
# 5. all_pos tests
# ===========================================================================


class TestAllPos:
    def test_returns_list(self, ram_db):
        assert isinstance(ram_db.all_pos(), list)

    def test_not_empty(self, ram_db):
        assert len(ram_db.all_pos()) > 0

    def test_distinct_count(self, ram_db):
        pos = ram_db.all_pos()
        assert len(pos) == 22

    def test_contains_noun(self, ram_db):
        assert any("noun (common)" in p for p in ram_db.all_pos())

    def test_no_duplicates(self, ram_db):
        pos = ram_db.all_pos()
        assert len(pos) == len(set(pos))


# ===========================================================================
# 6. update_meta / get_meta tests
# ===========================================================================


class TestMeta:
    def test_update_meta_sets_version(self, empty_db):
        empty_db.update_meta("9.99", "http://example.com/jmdict")
        assert empty_db.get_meta("jmdict.version") == "9.99"

    def test_update_meta_sets_url(self, empty_db):
        empty_db.update_meta("9.99", "http://example.com/jmdict")
        assert empty_db.get_meta("jmdict.url") == "http://example.com/jmdict"

    def test_update_meta_overwrites(self, empty_db):
        empty_db.update_meta("1.0", "http://a.com")
        empty_db.update_meta("2.0", "http://b.com")
        assert empty_db.get_meta("jmdict.version") == "2.0"
        assert empty_db.get_meta("jmdict.url") == "http://b.com"

    def test_get_meta_missing_key(self, empty_db):
        assert empty_db.get_meta("nonexistent.key") is None

    def test_seed_meta_on_init(self, empty_db):
        """A freshly opened DB should already have the default meta rows."""
        assert empty_db.get_meta("jmdict.version") is not None
        assert empty_db.get_meta("jmdict.url") is not None
        assert empty_db.get_meta("generator") == "jamdict"


# ===========================================================================
# 7. Context-manager protocol
# ===========================================================================


class TestContextManager:
    def test_context_manager_closes_db(self, xml_entries, tmp_path):
        db_path = str(tmp_path / "cm_test.db")
        with JMDictDB(db_path) as db:
            db.insert_entries(xml_entries)
            results = db.search("あの")
            assert len(results) == 2
        assert db._db.is_closed()

    def test_memory_db_via_context_manager(self, xml_entries):
        with JMDictDB(":memory:") as db:
            db.insert_entries(xml_entries)
            assert len(db.search("お菓子")) == 1


# ===========================================================================
# 8. JamdictPeewee runner tests
# ===========================================================================


class TestJamdictPeewee:
    def test_lookup_returns_lookup_result(self, jam):
        result = jam.lookup("あの")
        assert isinstance(result, LookupResult)

    def test_lookup_exact_kana(self, jam):
        result = jam.lookup("あの")
        assert len(result.entries) == 2

    def test_lookup_exact_kanji(self, jam):
        result = jam.lookup("お菓子")
        assert len(result.entries) == 1

    def test_lookup_wildcard(self, jam):
        result = jam.lookup("%あの%")
        assert len(result.entries) == 4

    def test_lookup_by_id(self, jam):
        result = jam.lookup("id#1001710")
        assert len(result.entries) == 1
        assert str(result.entries[0].idseq) == "1001710"

    def test_lookup_no_results(self, jam):
        result = jam.lookup("zzznomatch999")
        assert result.entries == []
        assert not result  # __bool__ is False when empty

    def test_lookup_result_truthy_when_found(self, jam):
        result = jam.lookup("あの")
        assert result

    def test_lookup_empty_query_raises(self, jam):
        with pytest.raises(ValueError):
            jam.lookup("")

    def test_lookup_bare_percent_raises(self, jam):
        with pytest.raises(ValueError):
            jam.lookup("%")

    def test_lookup_pos_filter(self, jam):
        all_r = jam.lookup("%あの%")
        filtered = jam.lookup("%あの%", pos=["pronoun"])
        assert len(filtered.entries) <= len(all_r.entries)
        for entry in filtered.entries:
            all_pos = [p for s in entry.senses for p in s.pos]
            assert any("pronoun" in p for p in all_pos)

    def test_lookup_iter_same_count(self, jam):
        list_count = len(jam.lookup("あの").entries)
        iter_count = sum(1 for _ in jam.lookup_iter("あの"))
        assert list_count == iter_count

    def test_lookup_iter_yields_jmdentry(self, jam):
        for entry in jam.lookup_iter("あの"):
            assert isinstance(entry, JMDEntry)

    def test_get_entry(self, jam):
        e = jam.get_entry(1001710)
        assert e is not None
        assert str(e.idseq) == "1001710"

    def test_get_entry_missing(self, jam):
        assert jam.get_entry(9999999999) is None

    def test_all_pos(self, jam):
        pos = jam.all_pos()
        assert isinstance(pos, list)
        assert len(pos) == 22

    def test_repr(self, jam):
        assert "JamdictPeewee" in repr(jam)

    def test_context_manager(self, tmp_path):
        db_path = str(tmp_path / "cm_runner.db")
        with JamdictPeewee(db_path=db_path, xml_path=str(MINI_JMD)) as runner:
            runner.import_data()
            result = runner.lookup("あの")
            assert len(result.entries) == 2
        assert runner._db is None  # closed and cleared by __exit__

    def test_import_data_missing_xml_raises(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "x.db"),
            xml_path="/nonexistent/path/JMdict.xml",
        )
        with pytest.raises(FileNotFoundError):
            runner.import_data()

    def test_import_data_no_xml_raises(self, tmp_path):
        runner = JamdictPeewee(db_path=str(tmp_path / "x.db"))
        with pytest.raises(ValueError):
            runner.import_data()


# ===========================================================================
# 9. Multiple concurrent instances (regression for singleton bug)
# ===========================================================================


class TestMultipleInstances:
    def test_two_memory_dbs_are_independent(self, xml_entries):
        """Two :memory: instances must not share data."""
        db1 = JMDictDB(":memory:")
        db2 = JMDictDB(":memory:")
        try:
            db1.insert_entries(xml_entries)
            # db2 is empty — searching it must return nothing
            assert db2.search("あの") == []
            # db1 should return results
            assert len(db1.search("あの")) == 2
        finally:
            db1.close()
            db2.close()

    def test_file_and_memory_db_independent(self, xml_entries, tmp_path):
        db_file = JMDictDB(str(tmp_path / "file.db"))
        db_mem = JMDictDB(":memory:")
        try:
            db_file.insert_entries(xml_entries)
            # memory DB is still empty
            assert db_mem.search("あの") == []
            assert len(db_file.search("あの")) == 2
        finally:
            db_file.close()
            db_mem.close()
