#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Phase 6 — public API parity tests.

Runs every significant public method side-by-side against:
  * OldJamdict  — the original puchikarui-backed implementation (util_old.py)
  * Jamdict     — the new peewee-backed implementation (util.py)

Both instances are populated from the same mini XML fixtures into separate
temporary file-backed databases so there is no shared state.

NOTE: :memory: is intentionally avoided for OldJamdict because the old
puchikarui MemorySource is broken in recent versions of puchikarui (as
documented in PEEWEE_MIGRATION.md).  File-backed DBs are used for both
backends so comparisons are fair.

A test passes when the *observable output* — types, lengths, key field values,
and serialisable dicts — is identical between the two implementations.

What is intentionally NOT compared
-----------------------------------
* Internal object identity / repr strings
* Logger names (old uses jmdict_sqlite, new uses jmdict_peewee)
* `idseq` integer vs string — JMDEntry.idseq has always been a plain string
  even in the old backend; tests normalise to int(idseq) where the test
  cares about numeric identity.
* `memory_mode` behaviour — the new backend silently ignores this flag.
* `ready` property — the old backend always returns False for :memory: (broken
  MemorySource); the new one returns True.  Not compared here.
* `get_entry` / `get_ne` with non-existent idseq — old backend returns an
  empty JMDEntry rather than None; new backend returns None.  Each backend's
  contract is tested individually rather than compared.
"""

import os
import tempfile
from pathlib import Path
from typing import List

import pytest

from jamdict.jmdict import JMDEntry
from jamdict.kanjidic2 import Character

# ---------------------------------------------------------------------------
# Implementations under test
# ---------------------------------------------------------------------------
from jamdict.old.util_old import Jamdict as OldJamdict
from jamdict.util import (
    IterLookupResult,
    Jamdict,  # new peewee-backed
    LookupResult,
)

# ---------------------------------------------------------------------------
# Paths to mini test fixtures
# ---------------------------------------------------------------------------

TEST_DIR = Path(os.path.realpath(__file__)).parent
TEST_DATA = TEST_DIR / "data"
MINI_JMD = TEST_DATA / "JMdict_mini.xml"
MINI_KD2 = TEST_DATA / "kanjidic2_mini.xml"
MINI_JMNE = TEST_DATA / "jmendict_mini.xml"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_old_jam(db_path: str) -> OldJamdict:
    """File-backed instance using the old puchikarui backend.

    auto_config=False prevents the fallback to jamdict_data's production DB
    when the temp file is freshly created.  kd2_file and jmnedict_file are
    set explicitly so all three dictionaries share the same SQLite file
    (matching the default jamdict layout).
    """
    jam = OldJamdict(
        db_path,
        kd2_file=db_path,
        jmnedict_file=db_path,
        jmd_xml_file=str(MINI_JMD),
        kd2_xml_file=str(MINI_KD2),
        jmnedict_xml_file=str(MINI_JMNE),
        auto_config=False,
    )
    jam.import_data()
    return jam


def _make_new_jam(db_path: str) -> Jamdict:
    """File-backed instance using the new peewee backend.

    auto_config=False prevents the fallback to jamdict_data's production DB.
    kd2_file and jmnedict_file are set explicitly so all three dictionaries
    share the same SQLite file (matching the default jamdict layout).
    """
    jam = Jamdict(
        db_path,
        kd2_file=db_path,
        jmnedict_file=db_path,
        jmd_xml_file=str(MINI_JMD),
        kd2_xml_file=str(MINI_KD2),
        jmnedict_xml_file=str(MINI_JMNE),
        auto_config=False,
    )
    jam.import_data()
    return jam


@pytest.fixture(scope="module")
def old_jam(tmp_path_factory) -> OldJamdict:
    db = tmp_path_factory.mktemp("old_db") / "old_jamdict_test.db"
    db.touch()  # must exist before OldJamdict opens it
    return _make_old_jam(str(db))


@pytest.fixture(scope="module")
def new_jam(tmp_path_factory) -> Jamdict:
    db = tmp_path_factory.mktemp("new_db") / "new_jamdict_test.db"
    db.touch()  # create the file so the path resolves without jamdict_data fallback
    return _make_new_jam(str(db))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry_key(e: JMDEntry) -> tuple:
    """A stable, comparable key for a JMDEntry — used for set comparisons."""
    kana = tuple(sorted(k.text for k in e.kana_forms))
    kanji = tuple(sorted(k.text for k in e.kanji_forms))
    gloss = tuple(sorted(g.text for s in e.senses for g in s.gloss))
    return (kana, kanji, gloss)


def _char_key(c: Character) -> str:
    return c.literal


def _entry_idseqs(entries) -> set:
    return {int(e.idseq) for e in entries}


def _entry_kana_set(entries) -> set:
    return {k.text for e in entries for k in e.kana_forms}


def _entry_kanji_set(entries) -> set:
    return {k.text for e in entries for k in e.kanji_forms}


def _entry_gloss_set(entries) -> set:
    return {g.text for e in entries for s in e.senses for g in s.gloss}


def _char_literals(chars) -> set:
    return {c.literal for c in chars}


def _name_kana_set(names) -> set:
    return {k.text for n in names for k in n.kana_forms}


def _name_kanji_set(names) -> set:
    return {k.text for n in names for k in n.kanji_forms}


# ===========================================================================
# 1. is_available / ready
# ===========================================================================


class TestAvailability:
    def test_is_available(self, old_jam, new_jam):
        assert old_jam.is_available() == new_jam.is_available()

    def test_ready_old_backend(self, old_jam):
        # The old backend uses os.path.isfile() which is True for file DBs
        assert old_jam.ready is True

    def test_ready_new_backend(self, new_jam):
        # The new backend also returns True for an existing file DB
        assert new_jam.ready is True

    def test_has_kd2(self, old_jam, new_jam):
        assert old_jam.has_kd2() == new_jam.has_kd2()

    def test_has_jmne(self, old_jam, new_jam):
        assert old_jam.has_jmne() == new_jam.has_jmne()


# ===========================================================================
# 2. lookup — basic word queries
# ===========================================================================


class TestLookupEntries:
    """Word-entry output (entries field) must be identical between backends."""

    # ---- helpers ----------------------------------------------------------

    def _compare_entries(self, old_res, new_res):
        """Assert that both results contain the same word entries."""
        old_ids = _entry_idseqs(old_res.entries)
        new_ids = _entry_idseqs(new_res.entries)
        assert old_ids == new_ids, f"idseq mismatch: old={old_ids}, new={new_ids}"

        # Also compare kana / kanji / gloss sets as a richer sanity check
        assert _entry_kana_set(old_res.entries) == _entry_kana_set(new_res.entries)
        assert _entry_kanji_set(old_res.entries) == _entry_kanji_set(new_res.entries)
        assert _entry_gloss_set(old_res.entries) == _entry_gloss_set(new_res.entries)

    # ---- tests ------------------------------------------------------------

    def test_exact_kana(self, old_jam, new_jam):
        old = old_jam.lookup("おみやげ")
        new = new_jam.lookup("おみやげ")
        self._compare_entries(old, new)

    def test_exact_kanji(self, old_jam, new_jam):
        old = old_jam.lookup("お土産")
        new = new_jam.lookup("お土産")
        self._compare_entries(old, new)

    def test_wildcard_prefix(self, old_jam, new_jam):
        old = old_jam.lookup("おこ%")
        new = new_jam.lookup("おこ%")
        self._compare_entries(old, new)

    def test_wildcard_suffix(self, old_jam, new_jam):
        old = old_jam.lookup("%やげ")
        new = new_jam.lookup("%やげ")
        self._compare_entries(old, new)

    def test_wildcard_infix(self, old_jam, new_jam):
        old = old_jam.lookup("お%産")
        new = new_jam.lookup("お%産")
        self._compare_entries(old, new)

    def test_id_lookup(self, old_jam, new_jam):
        # Pick a known idseq from the mini dict
        old = old_jam.lookup("id#1002490")
        new = new_jam.lookup("id#1002490")
        self._compare_entries(old, new)
        # Must have exactly one entry
        assert len(old.entries) == 1
        assert len(new.entries) == 1

    def test_no_results(self, old_jam, new_jam):
        old = old_jam.lookup("zzznomatch99")
        new = new_jam.lookup("zzznomatch99")
        assert len(old.entries) == 0
        assert len(new.entries) == 0

    def test_english_gloss_exact(self, old_jam, new_jam):
        old = old_jam.lookup("souvenir")
        new = new_jam.lookup("souvenir")
        self._compare_entries(old, new)

    def test_entry_count_matches(self, old_jam, new_jam):
        for query in ["おみやげ", "お%", "かえる", "あ%"]:
            old = old_jam.lookup(query)
            new = new_jam.lookup(query)
            assert len(old.entries) == len(new.entries), (
                f"Entry count mismatch for {query!r}: "
                f"old={len(old.entries)}, new={len(new.entries)}"
            )


# ===========================================================================
# 3. lookup — kanji character results
# ===========================================================================


class TestLookupChars:
    def test_chars_for_omiyage(self, old_jam, new_jam):
        old = old_jam.lookup("おみやげ")
        new = new_jam.lookup("おみやげ")
        # お土産 — the kana lookup expands to kanji forms; both should find 土,産
        assert _char_literals(old.chars) == _char_literals(new.chars)

    def test_chars_for_kanji_query(self, old_jam, new_jam):
        old = old_jam.lookup("お土産")
        new = new_jam.lookup("お土産")
        assert _char_literals(old.chars) == _char_literals(new.chars)

    def test_no_chars_for_kana_only(self, old_jam, new_jam):
        # A purely kana word with no kanji forms should produce no chars
        old = old_jam.lookup("おとそ")
        new = new_jam.lookup("おとそ")
        assert _char_literals(old.chars) == _char_literals(new.chars)

    def test_strict_lookup_vs_default(self, old_jam, new_jam):
        # strict_lookup=True restricts characters to those literally in query
        old_strict = old_jam.lookup("おみやげ", strict_lookup=True)
        new_strict = new_jam.lookup("おみやげ", strict_lookup=True)
        assert _char_literals(old_strict.chars) == _char_literals(new_strict.chars)

        old_default = old_jam.lookup("おみやげ", strict_lookup=False)
        new_default = new_jam.lookup("おみやげ", strict_lookup=False)
        assert _char_literals(old_default.chars) == _char_literals(new_default.chars)

    def test_lookup_chars_false(self, old_jam, new_jam):
        old = old_jam.lookup("お土産", lookup_chars=False)
        new = new_jam.lookup("お土産", lookup_chars=False)
        assert len(old.chars) == 0
        assert len(new.chars) == 0


# ===========================================================================
# 4. lookup — named-entity results
# ===========================================================================


class TestLookupNames:
    def test_names_for_surname(self, old_jam, new_jam):
        old = old_jam.lookup("surname")
        new = new_jam.lookup("surname")
        assert _name_kana_set(old.names) == _name_kana_set(new.names)
        assert _name_kanji_set(old.names) == _name_kanji_set(new.names)

    def test_names_for_place(self, old_jam, new_jam):
        old = old_jam.lookup("place")
        new = new_jam.lookup("place")
        assert _name_kana_set(old.names) == _name_kana_set(new.names)
        assert _name_kanji_set(old.names) == _name_kanji_set(new.names)

    def test_lookup_ne_false(self, old_jam, new_jam):
        old = old_jam.lookup("surname", lookup_ne=False)
        new = new_jam.lookup("surname", lookup_ne=False)
        assert len(old.names) == 0
        assert len(new.names) == 0

    def test_name_count_matches(self, old_jam, new_jam):
        for query in ["surname", "place", "company", "person"]:
            old = old_jam.lookup(query)
            new = new_jam.lookup(query)
            assert len(old.names) == len(new.names), (
                f"Name count mismatch for {query!r}: "
                f"old={len(old.names)}, new={len(new.names)}"
            )


# ===========================================================================
# 5. lookup — POS filter
# ===========================================================================


class TestLookupPos:
    def test_noun_filter(self, old_jam, new_jam):
        pos = ["noun (common) (futsuumeishi)"]
        old = old_jam.lookup("おみやげ", pos=pos)
        new = new_jam.lookup("おみやげ", pos=pos)
        assert _entry_idseqs(old.entries) == _entry_idseqs(new.entries)
        assert len(old.entries) == 1
        assert len(new.entries) == 1

    def test_pos_no_match(self, old_jam, new_jam):
        pos = ["intransitive verb"]
        old = old_jam.lookup("おみやげ", pos=pos)
        new = new_jam.lookup("おみやげ", pos=pos)
        assert len(old.entries) == 0
        assert len(new.entries) == 0

    def test_multiple_pos(self, old_jam, new_jam):
        pos = ["intransitive verb", "noun (common) (futsuumeishi)"]
        old = old_jam.lookup("おみやげ", pos=pos)
        new = new_jam.lookup("おみやげ", pos=pos)
        assert _entry_idseqs(old.entries) == _entry_idseqs(new.entries)
        assert len(old.entries) >= 1

    def test_wildcard_with_pos(self, old_jam, new_jam):
        pos = ["pronoun"]
        old = old_jam.lookup("", pos=pos)
        new = new_jam.lookup("", pos=pos)
        assert _entry_idseqs(old.entries) == _entry_idseqs(new.entries)

    def test_wildcard_percent_with_pos(self, old_jam, new_jam):
        pos = ["intransitive verb"]
        old = old_jam.lookup("%", pos=pos)
        new = new_jam.lookup("%", pos=pos)
        assert _entry_idseqs(old.entries) == _entry_idseqs(new.entries)


# ===========================================================================
# 6. LookupResult type and API surface
# ===========================================================================


class TestLookupResultType:
    def test_returns_lookup_result(self, old_jam, new_jam):
        """Both backends must return objects with the same key attributes."""
        old_res = old_jam.lookup("おみやげ")
        new_res = new_jam.lookup("おみやげ")
        # Duck-type check: both have entries / chars / names
        assert hasattr(old_res, "entries")
        assert hasattr(old_res, "chars")
        assert hasattr(old_res, "names")
        assert hasattr(new_res, "entries")
        assert hasattr(new_res, "chars")
        assert hasattr(new_res, "names")

    def test_to_dict_keys(self, old_jam, new_jam):
        old_d = old_jam.lookup("おみやげ").to_dict()
        new_d = new_jam.lookup("おみやげ").to_dict()
        assert set(old_d.keys()) == set(new_d.keys()) == {"entries", "chars", "names"}

    def test_to_dict_entries_count(self, old_jam, new_jam):
        old_d = old_jam.lookup("おみやげ").to_dict()
        new_d = new_jam.lookup("おみやげ").to_dict()
        assert len(old_d["entries"]) == len(new_d["entries"])

    def test_to_dict_chars_literals(self, old_jam, new_jam):
        old_d = old_jam.lookup("おみやげ").to_dict()
        new_d = new_jam.lookup("おみやげ").to_dict()
        old_lits = {c["literal"] for c in old_d["chars"]}
        new_lits = {c["literal"] for c in new_d["chars"]}
        assert old_lits == new_lits

    def test_to_dict_names_count(self, old_jam, new_jam):
        old_d = old_jam.lookup("surname").to_dict()
        new_d = new_jam.lookup("surname").to_dict()
        assert len(old_d["names"]) == len(new_d["names"])

    def test_to_json_deprecated_warning(self, new_jam):
        import warnings

        res = new_jam.lookup("おみやげ")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            res.to_json()
            assert any(issubclass(x.category, DeprecationWarning) for x in w)

    def test_text_repr_non_empty(self, old_jam, new_jam):
        old_txt = str(old_jam.lookup("おみやげ"))
        new_txt = str(new_jam.lookup("おみやげ"))
        assert old_txt != "Found nothing"
        assert new_txt != "Found nothing"


# ===========================================================================
# 7. Error cases
# ===========================================================================


class TestLookupErrors:
    def test_empty_query_raises(self, old_jam, new_jam):
        with pytest.raises(ValueError):
            old_jam.lookup("")
        with pytest.raises(ValueError):
            new_jam.lookup("")

    def test_bare_percent_raises(self, old_jam, new_jam):
        with pytest.raises(ValueError):
            old_jam.lookup("%")
        with pytest.raises(ValueError):
            new_jam.lookup("%")

    def test_empty_with_empty_pos_raises(self, old_jam, new_jam):
        with pytest.raises(ValueError):
            old_jam.lookup("", pos="")
        with pytest.raises(ValueError):
            new_jam.lookup("", pos="")


# ===========================================================================
# 8. all_pos
# ===========================================================================


class TestAllPos:
    def test_all_pos_same_set(self, old_jam, new_jam):
        old_pos = set(old_jam.all_pos())
        new_pos = set(new_jam.all_pos())
        assert old_pos == new_pos

    def test_all_pos_no_duplicates(self, old_jam, new_jam):
        old_pos = old_jam.all_pos()
        new_pos = new_jam.all_pos()
        assert len(old_pos) == len(set(old_pos))
        assert len(new_pos) == len(set(new_pos))

    def test_expected_pos_present(self, old_jam, new_jam):
        expected = {"noun (common) (futsuumeishi)", "pronoun", "intransitive verb"}
        old_pos = set(old_jam.all_pos())
        new_pos = set(new_jam.all_pos())
        assert expected.issubset(old_pos)
        assert expected.issubset(new_pos)


# ===========================================================================
# 9. all_ne_type
# ===========================================================================


class TestAllNeType:
    def test_all_ne_type_same_set(self, old_jam, new_jam):
        old_types = set(old_jam.all_ne_type())
        new_types = set(new_jam.all_ne_type())
        assert old_types == new_types

    def test_all_ne_type_no_duplicates(self, old_jam, new_jam):
        old_types = old_jam.all_ne_type()
        new_types = new_jam.all_ne_type()
        assert len(old_types) == len(set(old_types))
        assert len(new_types) == len(set(new_types))

    def test_expected_types_present(self, old_jam, new_jam):
        expected = {"surname", "place", "company"}
        old_types = set(old_jam.all_ne_type())
        new_types = set(new_jam.all_ne_type())
        assert expected.issubset(old_types)
        assert expected.issubset(new_types)


# ===========================================================================
# 10. get_entry
# ===========================================================================


class TestGetEntry:
    KNOWN_IDSEQ = 1002490  # おとそ in the mini dict

    def test_get_entry_returns_same_kana(self, old_jam, new_jam):
        old_e = old_jam.get_entry(self.KNOWN_IDSEQ)
        new_e = new_jam.get_entry(self.KNOWN_IDSEQ)
        assert old_e is not None
        assert new_e is not None
        old_kana = {k.text for k in old_e.kana_forms}
        new_kana = {k.text for k in new_e.kana_forms}
        assert old_kana == new_kana

    def test_get_entry_returns_same_gloss(self, old_jam, new_jam):
        old_e = old_jam.get_entry(self.KNOWN_IDSEQ)
        new_e = new_jam.get_entry(self.KNOWN_IDSEQ)
        old_gloss = {g.text for s in old_e.senses for g in s.gloss}
        new_gloss = {g.text for s in new_e.senses for g in s.gloss}
        assert old_gloss == new_gloss

    def test_get_entry_missing_new_returns_none(self, new_jam):
        # New peewee backend returns None for a non-existent idseq
        assert new_jam.get_entry(0) is None

    def test_get_entry_missing_old_returns_falsy(self, old_jam):
        # Old puchikarui backend returns an empty JMDEntry (falsy) for missing
        # idseq — the entry object exists but has no forms or senses
        result = old_jam.get_entry(0)
        # Either None or an empty entry with no kana/kanji/senses is acceptable
        if result is not None:
            assert not result.kana_forms
            assert not result.kanji_forms
            assert not result.senses

    def test_get_entry_to_dict_keys(self, old_jam, new_jam):
        old_e = old_jam.get_entry(self.KNOWN_IDSEQ)
        new_e = new_jam.get_entry(self.KNOWN_IDSEQ)
        assert set(old_e.to_dict().keys()) == set(new_e.to_dict().keys())

    def test_get_entry_idseq_normalises_to_int(self, old_jam, new_jam):
        old_e = old_jam.get_entry(self.KNOWN_IDSEQ)
        new_e = new_jam.get_entry(self.KNOWN_IDSEQ)
        assert int(old_e.idseq) == int(new_e.idseq) == self.KNOWN_IDSEQ


# ===========================================================================
# 11. get_char
# ===========================================================================


class TestGetChar:
    def test_get_char_literal(self, old_jam, new_jam):
        for lit in ["土", "産", "食"]:
            old_c = old_jam.get_char(lit)
            new_c = new_jam.get_char(lit)
            if old_c is None and new_c is None:
                continue  # char not in mini dict — both consistent
            assert old_c is not None, f"old missing char {lit!r}"
            assert new_c is not None, f"new missing char {lit!r}"
            assert old_c.literal == new_c.literal

    def test_get_char_stroke_count(self, old_jam, new_jam):
        for lit in ["土", "産"]:
            old_c = old_jam.get_char(lit)
            new_c = new_jam.get_char(lit)
            if old_c is None or new_c is None:
                continue
            assert old_c.stroke_count == new_c.stroke_count

    def test_get_char_missing_returns_none(self, old_jam, new_jam):
        old_c = old_jam.get_char("Ω")  # not a kanji
        new_c = new_jam.get_char("Ω")
        assert old_c is None
        assert new_c is None

    def test_get_char_to_dict_keys(self, old_jam, new_jam):
        old_c = old_jam.get_char("土")
        new_c = new_jam.get_char("土")
        if old_c is None or new_c is None:
            pytest.skip("土 not in mini dict")
        assert set(old_c.to_dict().keys()) == set(new_c.to_dict().keys())

    def test_get_char_readings_same(self, old_jam, new_jam):
        for lit in ["土", "産"]:
            old_c = old_jam.get_char(lit)
            new_c = new_jam.get_char(lit)
            if old_c is None or new_c is None:
                continue
            old_readings = {r.value for g in old_c.rm_groups for r in g.readings}
            new_readings = {r.value for g in new_c.rm_groups for r in g.readings}
            assert old_readings == new_readings, (
                f"Readings differ for {lit!r}: old={old_readings}, new={new_readings}"
            )


# ===========================================================================
# 12. get_ne
# ===========================================================================


class TestGetNe:
    SHENRON_IDSEQ = 5741815  # シェンロン / 神龍 in the mini jmnedict

    def test_get_ne_kana(self, old_jam, new_jam):
        old_ne = old_jam.get_ne(self.SHENRON_IDSEQ)
        new_ne = new_jam.get_ne(self.SHENRON_IDSEQ)
        assert old_ne is not None
        assert new_ne is not None
        old_kana = {k.text for k in old_ne.kana_forms}
        new_kana = {k.text for k in new_ne.kana_forms}
        assert old_kana == new_kana

    def test_get_ne_kanji(self, old_jam, new_jam):
        old_ne = old_jam.get_ne(self.SHENRON_IDSEQ)
        new_ne = new_jam.get_ne(self.SHENRON_IDSEQ)
        old_kanji = {k.text for k in old_ne.kanji_forms}
        new_kanji = {k.text for k in new_ne.kanji_forms}
        assert old_kanji == new_kanji

    def test_get_ne_missing_new_returns_none(self, new_jam):
        # New peewee backend returns None for a non-existent idseq
        assert new_jam.get_ne(0) is None

    def test_get_ne_missing_old_returns_falsy(self, old_jam):
        # Old puchikarui backend returns an empty JMDEntry (falsy) for missing
        result = old_jam.get_ne(0)
        if result is not None:
            assert not result.kana_forms
            assert not result.kanji_forms
            assert not result.senses

    def test_get_ne_to_dict_keys(self, old_jam, new_jam):
        old_ne = old_jam.get_ne(self.SHENRON_IDSEQ)
        new_ne = new_jam.get_ne(self.SHENRON_IDSEQ)
        assert set(old_ne.to_dict().keys()) == set(new_ne.to_dict().keys())


# ===========================================================================
# 13. lookup_iter
# ===========================================================================


class TestLookupIter:
    def test_entries_iterator_same_ids(self, old_jam, new_jam):
        old_res = old_jam.lookup_iter("おこ%", pos=["noun (common) (futsuumeishi)"])
        new_res = new_jam.lookup_iter("おこ%", pos=["noun (common) (futsuumeishi)"])
        old_entries = list(old_res.entries)
        new_entries = list(new_res.entries)
        assert _entry_idseqs(old_entries) == _entry_idseqs(new_entries)

    def test_chars_iterator_same_literals(self, old_jam, new_jam):
        old_res = old_jam.lookup_iter("お土産")
        new_res = new_jam.lookup_iter("お土産")
        old_chars = list(old_res.chars)
        new_chars = list(new_res.chars)
        assert {c.literal for c in old_chars} == {c.literal for c in new_chars}

    def test_names_iterator_same_kana(self, old_jam, new_jam):
        old_res = old_jam.lookup_iter("surname")
        new_res = new_jam.lookup_iter("surname")
        old_names = list(old_res.names)
        new_names = list(new_res.names)
        assert _name_kana_set(old_names) == _name_kana_set(new_names)

    def test_iter_returns_jmdentry(self, old_jam, new_jam):
        new_res = new_jam.lookup_iter("おみやげ")
        entries = list(new_res.entries)
        for e in entries:
            assert isinstance(e, JMDEntry)

    def test_iter_empty_query_raises(self, old_jam, new_jam):
        with pytest.raises(ValueError):
            old_jam.lookup_iter("")
        with pytest.raises(ValueError):
            new_jam.lookup_iter("")

    def test_lookup_iter_has_entries_chars_names_attrs(self, new_jam):
        res = new_jam.lookup_iter("お土産")
        assert hasattr(res, "entries")
        assert hasattr(res, "chars")
        assert hasattr(res, "names")


# ===========================================================================
# 14. JMDictXML / KanjiDic2XML / JMNEDictXML — XML-only path unchanged
# ===========================================================================


class TestXMLBackends:
    """The XML helpers (JMDictXML, KanjiDic2XML, JMNEDictXML) are shared
    unchanged between old and new util; they are tested here for regression."""

    def test_jmdict_xml_lookup(self):
        from jamdict.util import JMDictXML

        jmd = JMDictXML.from_file(str(MINI_JMD))
        results = jmd.lookup("おみやげ")
        assert results
        assert any(k.text == "おみやげ" for e in results for k in e.kana_forms)

    def test_jmdict_xml_id_lookup(self):
        from jamdict.util import JMDictXML

        jmd = JMDictXML.from_file(str(MINI_JMD))
        results = jmd.lookup("id#1002490")
        assert len(results) == 1
        assert int(results[0].idseq) == 1002490

    def test_kanjidic2_xml_lookup(self):
        from jamdict.util import KanjiDic2XML

        kd2 = KanjiDic2XML.from_file(str(MINI_KD2))
        c = kd2.lookup("土")
        assert c is not None
        assert c.literal == "土"

    def test_kanjidic2_xml_missing(self):
        from jamdict.util import KanjiDic2XML

        kd2 = KanjiDic2XML.from_file(str(MINI_KD2))
        assert kd2.lookup("Ω") is None

    def test_jmnedict_xml_lookup(self):
        from jamdict.util import JMNEDictXML

        jmne = JMNEDictXML.from_file(str(MINI_JMNE))
        results = jmne.lookup("シェンロン")
        assert results

    def test_xml_classes_importable_from_new_util(self):
        from jamdict.util import JMDictXML, JMNEDictXML, KanjiDic2XML

        assert JMDictXML is not None
        assert KanjiDic2XML is not None
        assert JMNEDictXML is not None


# ===========================================================================
# 15. __init__.py re-exports unchanged
# ===========================================================================


class TestPublicPackageExports:
    def test_jamdict_importable_from_package(self):
        from jamdict import Jamdict

        assert Jamdict is not None

    def test_jmdictxml_importable_from_package(self):
        from jamdict import JMDictXML

        assert JMDictXML is not None

    def test_kanjidic2xml_importable_from_package(self):
        from jamdict import KanjiDic2XML

        assert KanjiDic2XML is not None

    def test_jamdict_is_new_implementation(self):
        """Confirm the re-exported Jamdict is the new peewee-backed class."""
        from jamdict import Jamdict
        from jamdict.util import Jamdict as NewJamdict

        assert Jamdict is NewJamdict

    def test_memory_mode_kwarg_accepted(self):
        """memory_mode is a no-op in the new backend but must not raise."""
        from jamdict import Jamdict

        jam = Jamdict(":memory:", auto_config=False, memory_mode=True)
        assert jam.memory_mode is True  # attribute preserved for introspection


# ===========================================================================
# 16. New backend :memory: mode works (unlike old broken puchikarui path)
# ===========================================================================


class TestNewBackendMemoryMode:
    """These tests only run against the new backend, verifying that :memory:
    works correctly (the old backend's :memory: is broken by design)."""

    @pytest.fixture()
    def mem_jam(self) -> Jamdict:
        jam = Jamdict(
            ":memory:",
            kd2_file=":memory:",
            jmnedict_file=":memory:",
            jmd_xml_file=str(MINI_JMD),
            kd2_xml_file=str(MINI_KD2),
            jmnedict_xml_file=str(MINI_JMNE),
            auto_config=False,
        )
        jam.import_data()
        return jam

    def test_memory_mode_lookup_works(self, mem_jam):
        res = mem_jam.lookup("おみやげ")
        assert len(res.entries) == 1
        assert res.entries[0].kana_forms[0].text == "おみやげ"

    def test_memory_mode_chars_populated(self, mem_jam):
        res = mem_jam.lookup("おみやげ")
        assert {c.literal for c in res.chars} == {"土", "産"}

    def test_memory_mode_names_populated(self, mem_jam):
        res = mem_jam.lookup("surname")
        assert len(res.names) > 0

    def test_memory_mode_all_pos(self, mem_jam):
        pos = mem_jam.all_pos()
        assert "noun (common) (futsuumeishi)" in pos

    def test_memory_mode_get_entry(self, mem_jam):
        e = mem_jam.get_entry(1002490)
        assert e is not None
        assert {k.text for k in e.kana_forms} == {"おとそ"}
