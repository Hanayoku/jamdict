#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pytest test suite for Phase 5 of the peewee migration:
  - KanjiDic2DB  (jamdict/kanjidic2_peewee.py)
  - JMNEDictDB   (jamdict/jmnedict_peewee.py)
  - Extended JamdictPeewee with kd2 / jmne support
    (jamdict/jamdict_peewee.py)

Test data:
  test/data/kanjidic2_mini.xml  — small subset of KanjiDic2
  test/data/jmendict_mini.xml   — small subset of JMNEDict
  test/data/JMdict_mini.xml     — small subset of JMDict (for combined lookups)
"""

import os
from pathlib import Path

import pytest

from jamdict.jamdict_peewee import JamdictPeewee, LookupResult
from jamdict.jmdict import JMDEntry
from jamdict.jmnedict_peewee import JMNEDictDB
from jamdict.kanjidic2 import Character, Kanjidic2XMLParser
from jamdict.kanjidic2_peewee import KanjiDic2DB

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TEST_DIR = Path(os.path.realpath(__file__)).parent
TEST_DATA = TEST_DIR / "data"
MINI_KD2 = TEST_DATA / "kanjidic2_mini.xml"
MINI_JMNE = TEST_DATA / "jmendict_mini.xml"
MINI_JMD = TEST_DATA / "JMdict_mini.xml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_kd2():
    parser = Kanjidic2XMLParser()
    return parser.parse_file(str(MINI_KD2))


def _parse_jmne():
    from jamdict.old.util_old import JMNEDictXML

    xdb = JMNEDictXML.from_file(str(MINI_JMNE))
    return list(xdb)


# ===========================================================================
# KanjiDic2DB — fixtures
# ===========================================================================


@pytest.fixture(scope="module")
def kd2_data():
    """Parse kanjidic2_mini.xml once per module."""
    return _parse_kd2()


@pytest.fixture()
def kd2_ram(kd2_data):
    """Fresh :memory: KanjiDic2DB pre-populated with mini-XML characters."""
    db = KanjiDic2DB(":memory:")
    db.update_kd2_meta(
        kd2_data.file_version,
        kd2_data.database_version,
        kd2_data.date_of_creation,
    )
    db.insert_chars(kd2_data.characters)
    yield db
    db.close()


@pytest.fixture()
def kd2_empty():
    """Fresh, empty :memory: KanjiDic2DB."""
    db = KanjiDic2DB(":memory:")
    yield db
    db.close()


# ===========================================================================
# KanjiDic2DB — import tests
# ===========================================================================


class TestKanjiDic2Import:
    def test_char_count_matches_xml(self, kd2_ram, kd2_data):
        """Every character in the XML must be present in the DB."""
        chars = kd2_ram.all_chars()
        assert len(chars) == len(kd2_data.characters)

    def test_import_to_ram(self, kd2_data):
        """Importing into an in-memory DB must not raise."""
        db = KanjiDic2DB(":memory:")
        db.insert_chars(kd2_data.characters)
        db.close()

    def test_insert_char_returns_id(self, kd2_empty, kd2_data):
        """insert_char must propagate the DB-assigned ID back onto c.ID."""
        c = kd2_data.characters[0]
        original_literal = c.literal
        kd2_empty.insert_char(c)
        assert c.ID is not None
        assert c.ID > 0
        # round-trip: retrieve and check literal
        c2 = kd2_empty.get_char_by_id(c.ID)
        assert c2 is not None
        assert c2.literal == original_literal

    def test_insert_chars_all_retrievable(self, kd2_ram, kd2_data):
        """Every literal inserted must be retrievable by get_char."""
        for c_xml in kd2_data.characters:
            c_db = kd2_ram.get_char(c_xml.literal)
            assert c_db is not None, f"character {c_xml.literal!r} not found"


# ===========================================================================
# KanjiDic2DB — get_char / get_char_by_id tests
# ===========================================================================


class TestKanjiDic2GetChar:
    def test_get_char_returns_character_instance(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c = kd2_ram.get_char(lit)
        assert isinstance(c, Character)

    def test_get_char_correct_literal(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c = kd2_ram.get_char(lit)
        assert c.literal == lit

    def test_get_char_missing_returns_none(self, kd2_ram):
        assert kd2_ram.get_char("⿰") is None

    def test_get_char_by_id_returns_none_for_missing(self, kd2_ram):
        assert kd2_ram.get_char_by_id(999999) is None

    def test_get_char_has_readings(self, kd2_ram):
        """The 持 character must have at least one rm_group with readings."""
        c = kd2_ram.get_char("持")
        assert c is not None
        assert len(c.rm_groups) > 0
        assert len(c.rm_groups[0].readings) > 0

    def test_get_char_has_meanings(self, kd2_ram):
        """The 持 character must have at least one English meaning."""
        c = kd2_ram.get_char("持")
        assert c is not None
        meanings = c.meanings(english_only=True)
        assert len(meanings) > 0

    def test_get_char_on_reading(self, kd2_ram):
        c = kd2_ram.get_char("持")
        assert c is not None
        on_readings = [r.value for r in c.rm_groups[0].on_readings]
        assert "ジ" in on_readings

    def test_get_char_kun_readings(self, kd2_ram):
        c = kd2_ram.get_char("持")
        assert c is not None
        kun_readings = [r.value for r in c.rm_groups[0].kun_readings]
        assert "も.つ" in kun_readings

    def test_get_char_stroke_count(self, kd2_ram):
        c = kd2_ram.get_char("持")
        assert c is not None
        assert c.stroke_count is not None
        assert c.stroke_count > 0

    def test_get_char_codepoints(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c_xml = next(x for x in kd2_data.characters if x.literal == lit)
        c_db = kd2_ram.get_char(lit)
        assert len(c_db.codepoints) == len(c_xml.codepoints)

    def test_get_char_radicals(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c_xml = next(x for x in kd2_data.characters if x.literal == lit)
        c_db = kd2_ram.get_char(lit)
        assert len(c_db.radicals) == len(c_xml.radicals)

    def test_get_char_dic_refs_roundtrip(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c_xml = next(x for x in kd2_data.characters if x.literal == lit)
        c_db = kd2_ram.get_char(lit)
        assert len(c_db.dic_refs) == len(c_xml.dic_refs)

    def test_get_char_query_codes_roundtrip(self, kd2_ram, kd2_data):
        lit = kd2_data.characters[0].literal
        c_xml = next(x for x in kd2_data.characters if x.literal == lit)
        c_db = kd2_ram.get_char(lit)
        assert len(c_db.query_codes) == len(c_xml.query_codes)

    def test_to_dict_roundtrip(self, kd2_ram, kd2_data):
        """to_dict() output must be identical between XML-parsed and DB-retrieved char."""
        lit = kd2_data.characters[0].literal
        c_xml = next(x for x in kd2_data.characters if x.literal == lit)
        c_db = kd2_ram.get_char(lit)
        assert c_db.to_dict() == c_xml.to_dict()

    def test_all_chars_to_dict_roundtrip(self, kd2_ram, kd2_data):
        """Full to_dict() roundtrip for every character in the mini corpus."""
        xml_by_lit = {c.literal: c for c in kd2_data.characters}
        for c_db in kd2_ram.all_chars():
            c_xml = xml_by_lit[c_db.literal]
            assert c_db.to_dict() == c_xml.to_dict(), (
                f"to_dict() mismatch for {c_db.literal!r}"
            )


# ===========================================================================
# KanjiDic2DB — reading order
# ===========================================================================


class TestKanjiDic2ReadingOrder:
    def test_reading_order_preserved(self, kd2_ram):
        c = kd2_ram.get_char("持")
        rmg = c.rm_groups[0]
        assert [r.value for r in rmg.on_readings] == ["ジ"]
        assert [r.value for r in rmg.kun_readings] == ["も.つ", "-も.ち", "も.てる"]

    def test_other_readings_present(self, kd2_ram):
        c = kd2_ram.get_char("持")
        rmg = c.rm_groups[0]
        other_types = {r.r_type for r in rmg.other_readings}
        # should include at least pinyin / korean
        assert other_types & {"pinyin", "korean_r", "korean_h", "vietnam"}


# ===========================================================================
# KanjiDic2DB — metadata
# ===========================================================================


class TestKanjiDic2Meta:
    def test_seed_meta_on_init(self, kd2_empty):
        assert kd2_empty.get_meta("kanjidic2.version") == "1.6"

    def test_update_kd2_meta_sets_file_version(self, kd2_empty):
        kd2_empty.update_kd2_meta("4", "2024-01", "2024-01-01")
        assert kd2_empty.get_meta("kanjidic2.file_version") == "4"

    def test_update_kd2_meta_sets_db_version(self, kd2_empty):
        kd2_empty.update_kd2_meta("4", "2024-01", "2024-01-01")
        assert kd2_empty.get_meta("kanjidic2.database_version") == "2024-01"

    def test_update_kd2_meta_sets_date(self, kd2_empty):
        kd2_empty.update_kd2_meta("4", "2024-01", "2024-01-01")
        assert kd2_empty.get_meta("kanjidic2.date_of_creation") == "2024-01-01"

    def test_update_kd2_meta_overwrites(self, kd2_empty):
        kd2_empty.update_kd2_meta("1", "2020-01", "2020-01-01")
        kd2_empty.update_kd2_meta("2", "2021-01", "2021-06-15")
        assert kd2_empty.get_meta("kanjidic2.file_version") == "2"
        assert kd2_empty.get_meta("kanjidic2.database_version") == "2021-01"

    def test_get_meta_missing_key_returns_none(self, kd2_empty):
        assert kd2_empty.get_meta("no.such.key") is None

    def test_meta_populated_from_xml(self, kd2_ram, kd2_data):
        assert kd2_ram.get_meta("kanjidic2.file_version") == kd2_data.file_version
        assert (
            kd2_ram.get_meta("kanjidic2.database_version") == kd2_data.database_version
        )
        assert (
            kd2_ram.get_meta("kanjidic2.date_of_creation") == kd2_data.date_of_creation
        )


# ===========================================================================
# KanjiDic2DB — search_chars_iter
# ===========================================================================


class TestKanjiDic2SearchIter:
    def test_yields_found_chars(self, kd2_ram, kd2_data):
        literals = [c.literal for c in kd2_data.characters[:3]]
        result = list(kd2_ram.search_chars_iter(literals))
        assert len(result) == 3

    def test_skips_missing_literals(self, kd2_ram, kd2_data):
        literals = [kd2_data.characters[0].literal, "⿰"]
        result = list(kd2_ram.search_chars_iter(literals))
        assert len(result) == 1

    def test_yields_character_instances(self, kd2_ram, kd2_data):
        literals = [c.literal for c in kd2_data.characters[:2]]
        for c in kd2_ram.search_chars_iter(literals):
            assert isinstance(c, Character)

    def test_empty_input_yields_nothing(self, kd2_ram):
        result = list(kd2_ram.search_chars_iter([]))
        assert result == []


# ===========================================================================
# KanjiDic2DB — context manager + multiple instances
# ===========================================================================


class TestKanjiDic2ContextManager:
    def test_context_manager_closes_db(self):
        with KanjiDic2DB(":memory:") as db:
            assert db is not None
        assert db._db.is_closed()

    def test_two_memory_dbs_are_independent(self, kd2_data):
        with KanjiDic2DB(":memory:") as db1, KanjiDic2DB(":memory:") as db2:
            db1.insert_chars([kd2_data.characters[0]])
            # db2 is empty — should not see db1's character
            assert db2.get_char(kd2_data.characters[0].literal) is None
            assert db1.get_char(kd2_data.characters[0].literal) is not None

    def test_file_db_is_independent_from_memory(self, tmp_path, kd2_data):
        db_path = str(tmp_path / "kd2_test.db")
        with KanjiDic2DB(db_path) as fdb, KanjiDic2DB(":memory:") as mdb:
            fdb.insert_chars(kd2_data.characters)
            # memory DB is still empty
            assert len(mdb.all_chars()) == 0
            assert len(fdb.all_chars()) == len(kd2_data.characters)


# ===========================================================================
# JMNEDictDB — fixtures
# ===========================================================================


@pytest.fixture(scope="module")
def jmne_data():
    """Parse jmendict_mini.xml once per module."""
    return _parse_jmne()


@pytest.fixture()
def jmne_ram(jmne_data):
    """Fresh :memory: JMNEDictDB pre-populated with mini-XML entries."""
    db = JMNEDictDB(":memory:")
    db.insert_entries(jmne_data)
    yield db
    db.close()


@pytest.fixture()
def jmne_empty():
    """Fresh, empty :memory: JMNEDictDB."""
    db = JMNEDictDB(":memory:")
    yield db
    db.close()


# ===========================================================================
# JMNEDictDB — import tests
# ===========================================================================


class TestJMNEDictImport:
    def test_entry_count_matches_xml(self, jmne_ram, jmne_data):
        """Every entry parsed from XML must be present in the DB."""
        from jamdict.jmnedict_peewee import NEEntryModel

        with jmne_ram._db.bind_ctx(
            __import__("jamdict.jmnedict_peewee", fromlist=["ALL_MODELS"]).ALL_MODELS
        ):
            count = NEEntryModel.select().count()
        assert count == len(jmne_data)

    def test_import_to_ram(self, jmne_data):
        """Importing into an in-memory DB must not raise."""
        db = JMNEDictDB(":memory:")
        db.insert_entries(jmne_data)
        db.close()

    def test_insert_single_entry_roundtrip(self, jmne_empty, jmne_data):
        """A single inserted entry must be retrievable by get_ne."""
        entry = jmne_data[0]
        jmne_empty.insert_entry(entry)
        retrieved = jmne_empty.get_ne(int(entry.idseq))
        assert retrieved is not None
        assert int(retrieved.idseq) == int(entry.idseq)

    def test_all_idseqs_inserted(self, jmne_ram, jmne_data):
        """Every idseq from the XML must be findable by get_ne."""
        for e in jmne_data:
            r = jmne_ram.get_ne(int(e.idseq))
            assert r is not None, f"idseq {e.idseq} not found"


# ===========================================================================
# JMNEDictDB — get_ne
# ===========================================================================


class TestJMNEDictGetNe:
    def test_returns_jmdentry_instance(self, jmne_ram, jmne_data):
        idseq = int(jmne_data[0].idseq)
        e = jmne_ram.get_ne(idseq)
        assert isinstance(e, JMDEntry)

    def test_returns_none_for_missing(self, jmne_ram):
        assert jmne_ram.get_ne(99999999) is None

    def test_kanji_forms_roundtrip(self, jmne_ram, jmne_data):
        """Kanji forms must match the XML source."""
        for e_xml in jmne_data:
            e_db = jmne_ram.get_ne(int(e_xml.idseq))
            xml_kanjis = [k.text for k in e_xml.kanji_forms]
            db_kanjis = [k.text for k in e_db.kanji_forms]
            assert xml_kanjis == db_kanjis, f"kanji mismatch for idseq {e_xml.idseq}"

    def test_kana_forms_roundtrip(self, jmne_ram, jmne_data):
        """Kana forms must match the XML source."""
        for e_xml in jmne_data:
            e_db = jmne_ram.get_ne(int(e_xml.idseq))
            xml_kanas = [k.text for k in e_xml.kana_forms]
            db_kanas = [k.text for k in e_db.kana_forms]
            assert xml_kanas == db_kanas, f"kana mismatch for idseq {e_xml.idseq}"

    def test_glosses_roundtrip(self, jmne_ram, jmne_data):
        """Glosses for each sense must match the XML source."""
        for e_xml in jmne_data:
            e_db = jmne_ram.get_ne(int(e_xml.idseq))
            xml_glosses = [g.text for s in e_xml.senses for g in s.gloss]
            db_glosses = [g.text for s in e_db.senses for g in s.gloss]
            assert xml_glosses == db_glosses, f"gloss mismatch for idseq {e_xml.idseq}"

    def test_name_type_roundtrip(self, jmne_ram, jmne_data):
        """name_type lists must match the XML source."""
        from jamdict.jmdict import Translation

        for e_xml in jmne_data:
            e_db = jmne_ram.get_ne(int(e_xml.idseq))
            xml_types = [
                nt
                for s in e_xml.senses
                for nt in (s.name_type if isinstance(s, Translation) else [])
            ]
            db_types = [
                nt
                for s in e_db.senses
                for nt in (s.name_type if isinstance(s, Translation) else [])
            ]
            assert xml_types == db_types, f"name_type mismatch for idseq {e_xml.idseq}"

    def test_to_dict_roundtrip(self, jmne_ram, jmne_data):
        """to_dict() must be identical between XML-parsed and DB-retrieved entry."""
        for e_xml in jmne_data:
            e_xml.idseq = int(e_xml.idseq)
            e_db = jmne_ram.get_ne(int(e_xml.idseq))
            assert e_db.to_dict() == e_xml.to_dict(), (
                f"to_dict() mismatch for idseq {e_xml.idseq}"
            )

    def test_shenron_by_idseq(self, jmne_ram):
        shenron = jmne_ram.get_ne(5741815)
        assert shenron is not None
        assert shenron.idseq == 5741815

    def test_shenron_kanji(self, jmne_ram):
        shenron = jmne_ram.get_ne(5741815)
        kanjis = [k.text for k in shenron.kanji_forms]
        assert "神龍" in kanjis

    def test_shenron_kana(self, jmne_ram):
        shenron = jmne_ram.get_ne(5741815)
        kanas = [k.text for k in shenron.kana_forms]
        assert "シェンロン" in kanas


# ===========================================================================
# JMNEDictDB — search_ne
# ===========================================================================


class TestJMNEDictSearch:
    def test_search_by_idseq_prefix(self, jmne_ram):
        results = jmne_ram.search_ne("id#5741815")
        assert len(results) == 1
        assert results[0].idseq == 5741815

    def test_search_exact_kanji(self, jmne_ram):
        results = jmne_ram.search_ne("神龍")
        assert len(results) == 1
        assert results[0].idseq == 5741815

    def test_search_exact_kana(self, jmne_ram):
        results = jmne_ram.search_ne("シェンロン")
        assert len(results) == 1
        assert results[0].idseq == 5741815

    def test_search_exact_gloss(self, jmne_ram):
        # "spiritual" appears in shenron's gloss
        results = jmne_ram.search_ne("%spiritual%")
        idseqs = [r.idseq for r in results]
        assert 5741815 in idseqs

    def test_search_wildcard_kana_prefix(self, jmne_ram):
        results = jmne_ram.search_ne("しめ%")
        expected = [
            5000001,
            5000002,
            5000003,
            5000004,
            5000005,
            5000006,
            5000007,
            5000008,
            5000009,
        ]
        actual = [r.idseq for r in results]
        assert actual == expected

    def test_search_no_results(self, jmne_ram):
        results = jmne_ram.search_ne("ZZZNOMATCH")
        assert results == []

    def test_search_returns_list(self, jmne_ram):
        results = jmne_ram.search_ne("神龍")
        assert isinstance(results, list)

    def test_search_by_name_type(self, jmne_ram):
        """Searching by name_type string should return matching entries."""
        results = jmne_ram.search_ne("person")
        assert len(results) > 0

    def test_search_idseq_invalid_string(self, jmne_ram):
        """id# with non-integer suffix should return empty list."""
        results = jmne_ram.search_ne("id#notanumber")
        assert results == []


# ===========================================================================
# JMNEDictDB — search_ne_iter
# ===========================================================================


class TestJMNEDictSearchIter:
    def test_yields_same_count_as_search(self, jmne_ram):
        expected = jmne_ram.search_ne("しめ%")
        actual = list(jmne_ram.search_ne_iter("しめ%"))
        assert len(actual) == len(expected)

    def test_yields_jmdentry_objects(self, jmne_ram):
        for e in jmne_ram.search_ne_iter("神龍"):
            assert isinstance(e, JMDEntry)

    def test_no_results_yields_nothing(self, jmne_ram):
        assert list(jmne_ram.search_ne_iter("ZZZNOMATCH")) == []


# ===========================================================================
# JMNEDictDB — all_ne_type
# ===========================================================================


class TestJMNEDictAllNeType:
    def test_returns_list(self, jmne_ram):
        result = jmne_ram.all_ne_type()
        assert isinstance(result, list)

    def test_not_empty(self, jmne_ram):
        result = jmne_ram.all_ne_type()
        assert len(result) > 0

    def test_no_duplicates(self, jmne_ram):
        result = jmne_ram.all_ne_type()
        assert len(result) == len(set(result))

    def test_known_types_present(self, jmne_ram):
        result = set(jmne_ram.all_ne_type())
        # the mini corpus contains these types
        assert result & {"surname", "fem", "masc", "given", "place", "unclass"}

    def test_empty_db_returns_empty_list(self, jmne_empty):
        assert jmne_empty.all_ne_type() == []


# ===========================================================================
# JMNEDictDB — metadata
# ===========================================================================


class TestJMNEDictMeta:
    def test_seed_meta_on_init(self, jmne_empty):
        assert jmne_empty.get_meta("jmnedict.version") == "1.08"
        assert jmne_empty.get_meta("jmnedict.url") is not None

    def test_update_meta_sets_version(self, jmne_empty):
        jmne_empty.update_meta("2.0", "http://example.com", "2024-01-01")
        assert jmne_empty.get_meta("jmnedict.version") == "2.0"

    def test_update_meta_sets_url(self, jmne_empty):
        jmne_empty.update_meta("2.0", "http://example.com", "2024-01-01")
        assert jmne_empty.get_meta("jmnedict.url") == "http://example.com"

    def test_update_meta_sets_date(self, jmne_empty):
        jmne_empty.update_meta("2.0", "http://example.com", "2024-01-01")
        assert jmne_empty.get_meta("jmnedict.date") == "2024-01-01"

    def test_update_meta_overwrites(self, jmne_empty):
        jmne_empty.update_meta("1.0", "http://a.com")
        jmne_empty.update_meta("2.0", "http://b.com")
        assert jmne_empty.get_meta("jmnedict.version") == "2.0"
        assert jmne_empty.get_meta("jmnedict.url") == "http://b.com"

    def test_get_meta_missing_key_returns_none(self, jmne_empty):
        assert jmne_empty.get_meta("no.such.key") is None


# ===========================================================================
# JMNEDictDB — context manager + multiple instances
# ===========================================================================


class TestJMNEDictContextManager:
    def test_context_manager_closes_db(self):
        with JMNEDictDB(":memory:") as db:
            assert db is not None
        assert db._db.is_closed()

    def test_two_memory_dbs_are_independent(self, jmne_data):
        with JMNEDictDB(":memory:") as db1, JMNEDictDB(":memory:") as db2:
            db1.insert_entries([jmne_data[0]])
            assert db2.get_ne(int(jmne_data[0].idseq)) is None
            assert db1.get_ne(int(jmne_data[0].idseq)) is not None

    def test_file_db_independent_from_memory(self, tmp_path, jmne_data):
        db_path = str(tmp_path / "jmne_test.db")
        with JMNEDictDB(db_path) as fdb, JMNEDictDB(":memory:") as mdb:
            fdb.insert_entries(jmne_data)
            # memory DB is still empty
            assert mdb.get_ne(int(jmne_data[0].idseq)) is None
            assert fdb.get_ne(int(jmne_data[0].idseq)) is not None


# ===========================================================================
# Extended JamdictPeewee — fixtures
# ===========================================================================


@pytest.fixture(scope="module")
def full_jam_module(tmp_path_factory):
    """
    Module-scoped JamdictPeewee with all three dictionaries configured
    and imported.  tmp_path_factory gives us a stable directory for the
    session.
    """
    d = tmp_path_factory.mktemp("jamfull")
    runner = JamdictPeewee(
        db_path=str(d / "jmdict.db"),
        xml_path=str(MINI_JMD),
        kd2_db_path=str(d / "kanjidic2.db"),
        kd2_xml_path=str(MINI_KD2),
        jmne_db_path=str(d / "jmnedict.db"),
        jmne_xml_path=str(MINI_JMNE),
    )
    runner.import_data()
    yield runner
    runner.close()


@pytest.fixture()
def jmd_only_jam(tmp_path):
    """JamdictPeewee with JMDict only (no KanjiDic2, no JMNEDict)."""
    runner = JamdictPeewee(
        db_path=str(tmp_path / "jmdict.db"),
        xml_path=str(MINI_JMD),
    )
    runner.import_data(kanjidic2=False, jmnedict=False)
    yield runner
    runner.close()


# ===========================================================================
# Extended JamdictPeewee — import
# ===========================================================================


class TestJamdictPeeweeExtendedImport:
    def test_import_data_all_three(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            kd2_xml_path=str(MINI_KD2),
            jmne_db_path=str(tmp_path / "jmne.db"),
            jmne_xml_path=str(MINI_JMNE),
        )
        runner.import_data()
        runner.close()

    def test_import_data_jmdict_only_flag(self, tmp_path):
        """When kanjidic2=False and jmnedict=False, only JMDict is imported."""
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            kd2_xml_path=str(MINI_KD2),
            jmne_db_path=str(tmp_path / "jmne.db"),
            jmne_xml_path=str(MINI_JMNE),
        )
        runner.import_data(kanjidic2=False, jmnedict=False)
        # KanjiDic2 DB exists but has no characters
        assert runner.kd2_db is not None
        assert runner.kd2_db.all_chars() == []
        runner.close()

    def test_import_kd2_missing_xml_raises(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            kd2_xml_path="/nonexistent/kanjidic2.xml",
        )
        with pytest.raises(FileNotFoundError):
            runner.import_data(jmdict=False, jmnedict=False)
        runner.close()

    def test_import_jmne_missing_xml_raises(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            jmne_db_path=str(tmp_path / "jmne.db"),
            jmne_xml_path="/nonexistent/jmnedict.xml",
        )
        with pytest.raises(FileNotFoundError):
            runner.import_data(jmdict=False, kanjidic2=False)
        runner.close()


# ===========================================================================
# Extended JamdictPeewee — KanjiDic2 accessors
# ===========================================================================


class TestJamdictPeeweeKanjiDic2:
    def test_get_char_returns_character(self, full_jam_module):
        c = full_jam_module.get_char("持")
        assert isinstance(c, Character)
        assert c.literal == "持"

    def test_get_char_missing_returns_none(self, full_jam_module):
        assert full_jam_module.get_char("⿰") is None

    def test_get_char_by_id_returns_character(self, full_jam_module):
        c = full_jam_module.get_char("持")
        assert c is not None
        c2 = full_jam_module.get_char_by_id(c.ID)
        assert c2 is not None
        assert c2.literal == "持"

    def test_all_chars_non_empty(self, full_jam_module, kd2_data):
        chars = full_jam_module.all_chars()
        assert len(chars) == len(kd2_data.characters)

    def test_get_char_no_kd2_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="kd2_db_path"):
            jmd_only_jam.get_char("持")

    def test_get_char_by_id_no_kd2_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="kd2_db_path"):
            jmd_only_jam.get_char_by_id(1)

    def test_all_chars_no_kd2_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="kd2_db_path"):
            jmd_only_jam.all_chars()


# ===========================================================================
# Extended JamdictPeewee — JMNEDict accessors
# ===========================================================================


class TestJamdictPeeweeJMNEDict:
    def test_get_ne_returns_jmdentry(self, full_jam_module):
        e = full_jam_module.get_ne(5741815)
        assert isinstance(e, JMDEntry)
        assert e.idseq == 5741815

    def test_get_ne_missing_returns_none(self, full_jam_module):
        assert full_jam_module.get_ne(99999999) is None

    def test_search_ne_exact(self, full_jam_module):
        results = full_jam_module.search_ne("神龍")
        assert len(results) == 1
        assert results[0].idseq == 5741815

    def test_search_ne_wildcard(self, full_jam_module):
        results = full_jam_module.search_ne("しめ%")
        assert len(results) == 9

    def test_search_ne_iter_yields_jmdentry(self, full_jam_module):
        for e in full_jam_module.search_ne_iter("神龍"):
            assert isinstance(e, JMDEntry)

    def test_all_ne_type_non_empty(self, full_jam_module):
        types = full_jam_module.all_ne_type()
        assert len(types) > 0

    def test_get_ne_no_jmne_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="jmne_db_path"):
            jmd_only_jam.get_ne(5741815)

    def test_search_ne_no_jmne_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="jmne_db_path"):
            jmd_only_jam.search_ne("神龍")

    def test_search_ne_iter_no_jmne_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="jmne_db_path"):
            list(jmd_only_jam.search_ne_iter("神龍"))

    def test_all_ne_type_no_jmne_raises(self, jmd_only_jam):
        with pytest.raises(RuntimeError, match="jmne_db_path"):
            jmd_only_jam.all_ne_type()


# ===========================================================================
# Extended JamdictPeewee — combined lookup (LookupResult.chars + .names)
# ===========================================================================


class TestJamdictPeeweeExtendedLookup:
    def test_lookup_result_has_chars(self, full_jam_module):
        """lookup() on a kanji-containing query must populate result.chars."""
        result = full_jam_module.lookup("持")
        assert isinstance(result.chars, list)
        # 持 is in kanjidic2_mini.xml
        assert len(result.chars) > 0
        assert isinstance(result.chars[0], Character)

    def test_lookup_result_chars_correct_literal(self, full_jam_module):
        result = full_jam_module.lookup("持")
        literals = [c.literal for c in result.chars]
        assert "持" in literals

    def test_lookup_result_has_names(self, full_jam_module):
        """lookup() on a query that matches a named entity populates result.names."""
        result = full_jam_module.lookup("神龍")
        assert isinstance(result.names, list)
        assert len(result.names) > 0

    def test_lookup_result_names_correct_idseq(self, full_jam_module):
        result = full_jam_module.lookup("神龍")
        idseqs = [e.idseq for e in result.names]
        assert 5741815 in idseqs

    def test_lookup_no_kd2_empty_chars(self, jmd_only_jam):
        """Without KanjiDic2 configured, result.chars must always be empty."""
        result = jmd_only_jam.lookup("持")
        assert result.chars == []

    def test_lookup_no_jmne_empty_names(self, jmd_only_jam):
        """Without JMNEDict configured, result.names must always be empty."""
        result = jmd_only_jam.lookup("神龍")
        assert result.names == []

    def test_lookup_result_bool_true_via_chars(self, full_jam_module):
        """LookupResult is truthy when only chars is populated."""
        result = full_jam_module.lookup("持")
        # Even if word entries is empty, chars makes it truthy
        result_only_chars = LookupResult(entries=[], chars=result.chars)
        assert bool(result_only_chars) is True

    def test_lookup_result_bool_true_via_names(self, full_jam_module):
        result = full_jam_module.lookup("神龍")
        result_only_names = LookupResult(entries=[], names=result.names)
        assert bool(result_only_names) is True

    def test_lookup_result_bool_false_when_all_empty(self):
        result = LookupResult(entries=[], chars=[], names=[])
        assert bool(result) is False

    def test_lookup_result_repr_includes_all_counts(self):
        result = LookupResult(
            entries=[object()],  # type: ignore[list-item]
            chars=[object()],  # type: ignore[list-item]
            names=[object(), object()],  # type: ignore[list-item]
        )
        r = repr(result)
        assert "entries=1" in r
        assert "chars=1" in r
        assert "names=2" in r


# ===========================================================================
# Extended JamdictPeewee — resource management
# ===========================================================================


class TestJamdictPeeweeExtendedResourceMgmt:
    def test_close_closes_all_dbs(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            kd2_xml_path=str(MINI_KD2),
            jmne_db_path=str(tmp_path / "jmne.db"),
            jmne_xml_path=str(MINI_JMNE),
        )
        runner.import_data()
        # Force lazy opens
        _ = runner.db
        _ = runner.kd2_db
        _ = runner.jmne_db
        runner.close()
        assert runner._db is None
        assert runner._kd2_db is None
        assert runner._jmne_db is None

    def test_context_manager_closes_all(self, tmp_path):
        with JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            kd2_xml_path=str(MINI_KD2),
            jmne_db_path=str(tmp_path / "jmne.db"),
            jmne_xml_path=str(MINI_JMNE),
        ) as runner:
            runner.import_data()
            _ = runner.kd2_db
            _ = runner.jmne_db
        assert runner._db is None
        assert runner._kd2_db is None
        assert runner._jmne_db is None

    def test_repr_includes_all_paths(self, tmp_path):
        runner = JamdictPeewee(
            db_path=str(tmp_path / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(tmp_path / "kd2.db"),
            jmne_db_path=str(tmp_path / "jmne.db"),
        )
        r = repr(runner)
        assert "kd2_db=" in r
        assert "jmne_db=" in r
        runner.close()

    def test_kd2_db_property_returns_none_when_not_configured(self, jmd_only_jam):
        assert jmd_only_jam.kd2_db is None

    def test_jmne_db_property_returns_none_when_not_configured(self, jmd_only_jam):
        assert jmd_only_jam.jmne_db is None

    def test_multiple_full_instances_independent(self, tmp_path, kd2_data, jmne_data):
        """Two fully-configured JamdictPeewee instances must not interfere."""
        d1 = tmp_path / "inst1"
        d2 = tmp_path / "inst2"
        d1.mkdir()
        d2.mkdir()

        jam1 = JamdictPeewee(
            db_path=str(d1 / "jmdict.db"),
            xml_path=str(MINI_JMD),
            kd2_db_path=str(d1 / "kd2.db"),
            kd2_xml_path=str(MINI_KD2),
            jmne_db_path=str(d1 / "jmne.db"),
            jmne_xml_path=str(MINI_JMNE),
        )
        jam1.import_data()

        # jam2 is empty — no import
        jam2 = JamdictPeewee(
            db_path=str(d2 / "jmdict.db"),
            kd2_db_path=str(d2 / "kd2.db"),
            jmne_db_path=str(d2 / "jmne.db"),
        )

        try:
            assert jam1.get_char("持") is not None
            assert jam2.get_char("持") is None

            assert jam1.get_ne(5741815) is not None
            assert jam2.get_ne(5741815) is None
        finally:
            jam1.close()
            jam2.close()
