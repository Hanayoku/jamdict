# -*- coding: utf-8 -*-

"""
JamdictPeewee — a clean Jamdict-equivalent that uses the peewee-backed
JMDictDB, KanjiDic2DB, and JMNEDictDB stores.

Scope:
  - JMDict word lookup
  - KanjiDic2 character lookup
  - JMNEDict named-entity lookup
  - import from XML for all three dictionaries

The existing util.py / Jamdict class and all puchikarui-backed code are left
completely untouched.
"""

# This code is a part of jamdict library: https://github.com/neocl/jamdict
# :copyright: (c) 2016 Le Tuan Anh <tuananh.ke@gmail.com>
# :license: MIT, see LICENSE for more details.

import logging
import os
from typing import Iterator, List, Optional

from .jmdict import JMDEntry, JMDictXMLParser
from .jmdict_peewee import JMDictDB
from .jmnedict_peewee import JMNEDictDB
from .kanjidic2 import Character, Kanjidic2XMLParser
from .kanjidic2_peewee import KanjiDic2DB


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lookup result
# ---------------------------------------------------------------------------


class LookupResult:
    """
    Holds the result of a :meth:`JamdictPeewee.lookup` call.

    Attributes
    ----------
    entries : list[JMDEntry]
        Matched word entries (JMDict).
    chars : list[Character]
        Matched kanji characters (KanjiDic2).  Empty when the KanjiDic2
        database is not configured.
    names : list[JMDEntry]
        Matched named-entity entries (JMNEDict).  Empty when the JMNEDict
        database is not configured.
    """

    def __init__(
        self,
        entries: List[JMDEntry],
        chars: Optional[List[Character]] = None,
        names: Optional[List[JMDEntry]] = None,
    ):
        self.entries: List[JMDEntry] = entries if entries is not None else []
        self.chars: List[Character] = chars if chars is not None else []
        self.names: List[JMDEntry] = names if names is not None else []

    def __repr__(self) -> str:
        return (
            f"LookupResult("
            f"entries={len(self.entries)}, "
            f"chars={len(self.chars)}, "
            f"names={len(self.names)})"
        )

    def __bool__(self) -> bool:
        return bool(self.entries) or bool(self.chars) or bool(self.names)


# ---------------------------------------------------------------------------
# JamdictPeewee
# ---------------------------------------------------------------------------


class JamdictPeewee:
    """
    Clean peewee-backed Jamdict runner.

    Supports JMDict word lookup, KanjiDic2 character lookup, and JMNEDict
    named-entity lookup.

    Typical usage::

        jam = JamdictPeewee(
            db_path="jmdict.db",
            xml_path="JMdict_e.xml",
            kd2_db_path="kanjidic2.db",
            kd2_xml_path="kanjidic2.xml",
            jmne_db_path="jmnedict.db",
            jmne_xml_path="JMnedict.xml",
        )

        # First run — import XML into the SQLite DBs
        jam.import_data()

        # Subsequent runs — query directly
        result = jam.lookup("食べる")
        for entry in result.entries:
            print(entry)

        # Look up a single kanji character
        char = jam.get_char("持")

        # Look up a named entity
        ne = jam.get_ne(5741815)

    Parameters
    ----------
    db_path:
        Path to the JMDict SQLite database file, or ``':memory:'``.
    xml_path:
        Optional path to a JMDict XML source file.  Required only when calling
        :meth:`import_data` for JMDict.
    kd2_db_path:
        Optional path to the KanjiDic2 SQLite database file, or
        ``':memory:'``.  When omitted, KanjiDic2 functionality is disabled.
    kd2_xml_path:
        Optional path to a KanjiDic2 XML source file.  Required only when
        calling :meth:`import_data` for KanjiDic2.
    jmne_db_path:
        Optional path to the JMNEDict SQLite database file, or
        ``':memory:'``.  When omitted, JMNEDict functionality is disabled.
    jmne_xml_path:
        Optional path to a JMNEDict XML source file.  Required only when
        calling :meth:`import_data` for JMNEDict.
    """

    def __init__(
        self,
        db_path: str,
        xml_path: Optional[str] = None,
        kd2_db_path: Optional[str] = None,
        kd2_xml_path: Optional[str] = None,
        jmne_db_path: Optional[str] = None,
        jmne_xml_path: Optional[str] = None,
    ):
        self._db_path = db_path
        self._xml_path = xml_path
        self._kd2_db_path = kd2_db_path
        self._kd2_xml_path = kd2_xml_path
        self._jmne_db_path = jmne_db_path
        self._jmne_xml_path = jmne_xml_path

        self._db: Optional[JMDictDB] = None
        self._kd2_db: Optional[KanjiDic2DB] = None
        self._jmne_db: Optional[JMNEDictDB] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def db(self) -> JMDictDB:
        """Lazily open the JMDict database on first access."""
        if self._db is None:
            self._db = JMDictDB(self._db_path)
        return self._db

    @property
    def kd2_db(self) -> Optional[KanjiDic2DB]:
        """
        Lazily open the KanjiDic2 database on first access.

        Returns None if no ``kd2_db_path`` was configured.
        """
        if self._kd2_db_path is None:
            return None
        if self._kd2_db is None:
            self._kd2_db = KanjiDic2DB(self._kd2_db_path)
        return self._kd2_db

    @property
    def jmne_db(self) -> Optional[JMNEDictDB]:
        """
        Lazily open the JMNEDict database on first access.

        Returns None if no ``jmne_db_path`` was configured.
        """
        if self._jmne_db_path is None:
            return None
        if self._jmne_db is None:
            self._jmne_db = JMNEDictDB(self._jmne_db_path)
        return self._jmne_db

    def _parse_jmdict_xml(self) -> List[JMDEntry]:
        """Parse the configured JMDict XML file and return a list of JMDEntry objects."""
        if not self._xml_path:
            raise ValueError("xml_path is required for JMDict XML import")
        xml_path = os.path.abspath(os.path.expanduser(self._xml_path))
        if not os.path.isfile(xml_path):
            raise FileNotFoundError(f"JMDict XML not found: {xml_path}")
        getLogger().info("Parsing JMDict XML: %s", xml_path)
        parser = JMDictXMLParser()
        return parser.parse_file(xml_path)

    def _parse_kd2_xml(self):
        """Parse the configured KanjiDic2 XML file and return a KanjiDic2 object."""
        if not self._kd2_xml_path:
            raise ValueError("kd2_xml_path is required for KanjiDic2 XML import")
        xml_path = os.path.abspath(os.path.expanduser(self._kd2_xml_path))
        if not os.path.isfile(xml_path):
            raise FileNotFoundError(f"KanjiDic2 XML not found: {xml_path}")
        getLogger().info("Parsing KanjiDic2 XML: %s", xml_path)
        parser = Kanjidic2XMLParser()
        return parser.parse_file(xml_path)

    def _parse_jmne_xml(self) -> List[JMDEntry]:
        """Parse the configured JMNEDict XML file and return a list of JMDEntry objects."""
        if not self._jmne_xml_path:
            raise ValueError("jmne_xml_path is required for JMNEDict XML import")
        xml_path = os.path.abspath(os.path.expanduser(self._jmne_xml_path))
        if not os.path.isfile(xml_path):
            raise FileNotFoundError(f"JMNEDict XML not found: {xml_path}")
        getLogger().info("Parsing JMNEDict XML: %s", xml_path)
        # JMNEDict XML uses the same parser infrastructure as JMDict
        from .old.util_old import JMNEDictXML

        xdb = JMNEDictXML.from_file(xml_path)
        return list(xdb)

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def import_data(
        self,
        jmdict: bool = True,
        kanjidic2: bool = True,
        jmnedict: bool = True,
    ) -> None:
        """
        Parse the configured XML source files and bulk-insert all entries
        into the respective SQLite databases.

        By default all three dictionaries are imported.  Pass ``jmdict=False``,
        ``kanjidic2=False``, or ``jmnedict=False`` to skip individual sources.

        If the database already contains entries this method inserts
        duplicates — call this only on a fresh (or just-wiped) database.
        """
        if jmdict:
            entries = self._parse_jmdict_xml()
            getLogger().info(
                "Importing %d JMDict entries into %s", len(entries), self._db_path
            )
            self.db.insert_entries(entries)
            getLogger().info("JMDict import complete")

        if kanjidic2 and self._kd2_xml_path and self.kd2_db is not None:
            kd2 = self._parse_kd2_xml()
            getLogger().info(
                "Importing %d KanjiDic2 characters into %s",
                len(kd2),
                self._kd2_db_path,
            )
            self.kd2_db.update_kd2_meta(
                kd2.file_version,
                kd2.database_version,
                kd2.date_of_creation,
            )
            self.kd2_db.insert_chars(kd2.characters)
            getLogger().info("KanjiDic2 import complete")

        if jmnedict and self._jmne_xml_path and self.jmne_db is not None:
            ne_entries = self._parse_jmne_xml()
            getLogger().info(
                "Importing %d JMNEDict entries into %s",
                len(ne_entries),
                self._jmne_db_path,
            )
            self.jmne_db.insert_entries(ne_entries)
            getLogger().info("JMNEDict import complete")

    # ------------------------------------------------------------------
    # JMDict query
    # ------------------------------------------------------------------

    def lookup(self, query: str, pos=None) -> LookupResult:
        """
        Search for word entries matching *query*.

        Searches JMDict (word entries) always.  When KanjiDic2 and/or
        JMNEDict databases are configured, those are searched too and the
        results are surfaced in ``result.chars`` and ``result.names``.

        Parameters
        ----------
        query:
            Text to search.  Supports:

            * Exact kana/kanji/gloss match: ``"食べる"``
            * SQL LIKE wildcards (``%``, ``_``): ``"食べ%る"``
            * ID lookup: ``"id#1234567"``
        pos:
            Optional list of part-of-speech strings to filter JMDict results.

        Returns
        -------
        LookupResult
        """
        if not query or (query == "%" and not pos):
            raise ValueError("query cannot be empty or bare '%' without a pos filter")
        entries = self.db.search(query, pos=pos)

        # KanjiDic2 — only meaningful for single-character literal lookups
        chars: List[Character] = []
        if self.kd2_db is not None:
            for ch in query:
                c = self.kd2_db.get_char(ch)
                if c is not None:
                    chars.append(c)

        # JMNEDict
        names: List[JMDEntry] = []
        if self.jmne_db is not None:
            names = self.jmne_db.search_ne(query)

        return LookupResult(entries, chars=chars, names=names)

    def lookup_iter(self, query: str, pos=None) -> Iterator[JMDEntry]:
        """
        Yield JMDict word entries matching *query* one at a time.

        Useful for large result sets where you do not want to materialise
        the full list in memory.  Does not include KanjiDic2 or JMNEDict
        results — use :meth:`lookup` for those.
        """
        if not query or (query == "%" and not pos):
            raise ValueError("query cannot be empty or bare '%' without a pos filter")
        yield from self.db.search_iter(query, pos=pos)

    def get_entry(self, idseq: int) -> Optional[JMDEntry]:
        """Return the JMDict entry with the given *idseq*, or None if not found."""
        return self.db.get_entry(idseq)

    def all_pos(self) -> List[str]:
        """Return a list of all distinct part-of-speech tags in the JMDict database."""
        return self.db.all_pos()

    # ------------------------------------------------------------------
    # KanjiDic2 query
    # ------------------------------------------------------------------

    def get_char(self, literal: str) -> Optional[Character]:
        """
        Return the KanjiDic2 Character for the given *literal*, or None.

        Raises ``RuntimeError`` if no KanjiDic2 database is configured.
        """
        if self.kd2_db is None:
            raise RuntimeError(
                "KanjiDic2 database is not configured. "
                "Pass kd2_db_path= to JamdictPeewee()."
            )
        return self.kd2_db.get_char(literal)

    def get_char_by_id(self, cid: int) -> Optional[Character]:
        """
        Return the KanjiDic2 Character with the given internal *cid*, or None.

        Raises ``RuntimeError`` if no KanjiDic2 database is configured.
        """
        if self.kd2_db is None:
            raise RuntimeError(
                "KanjiDic2 database is not configured. "
                "Pass kd2_db_path= to JamdictPeewee()."
            )
        return self.kd2_db.get_char_by_id(cid)

    def all_chars(self) -> List[Character]:
        """
        Return all KanjiDic2 characters in the database as a list.

        Raises ``RuntimeError`` if no KanjiDic2 database is configured.
        """
        if self.kd2_db is None:
            raise RuntimeError(
                "KanjiDic2 database is not configured. "
                "Pass kd2_db_path= to JamdictPeewee()."
            )
        return self.kd2_db.all_chars()

    # ------------------------------------------------------------------
    # JMNEDict query
    # ------------------------------------------------------------------

    def get_ne(self, idseq: int) -> Optional[JMDEntry]:
        """
        Return the JMNEDict named-entity entry with the given *idseq*, or None.

        Raises ``RuntimeError`` if no JMNEDict database is configured.
        """
        if self.jmne_db is None:
            raise RuntimeError(
                "JMNEDict database is not configured. "
                "Pass jmne_db_path= to JamdictPeewee()."
            )
        return self.jmne_db.get_ne(idseq)

    def search_ne(self, query: str) -> List[JMDEntry]:
        """
        Return all JMNEDict named-entity entries matching *query* as a list.

        Raises ``RuntimeError`` if no JMNEDict database is configured.
        """
        if self.jmne_db is None:
            raise RuntimeError(
                "JMNEDict database is not configured. "
                "Pass jmne_db_path= to JamdictPeewee()."
            )
        return self.jmne_db.search_ne(query)

    def search_ne_iter(self, query: str) -> Iterator[JMDEntry]:
        """
        Yield JMNEDict named-entity entries matching *query* one at a time.

        Raises ``RuntimeError`` if no JMNEDict database is configured.
        """
        if self.jmne_db is None:
            raise RuntimeError(
                "JMNEDict database is not configured. "
                "Pass jmne_db_path= to JamdictPeewee()."
            )
        yield from self.jmne_db.search_ne_iter(query)

    def all_ne_type(self) -> List[str]:
        """
        Return a list of all distinct name-type tags in the JMNEDict database.

        Raises ``RuntimeError`` if no JMNEDict database is configured.
        """
        if self.jmne_db is None:
            raise RuntimeError(
                "JMNEDict database is not configured. "
                "Pass jmne_db_path= to JamdictPeewee()."
            )
        return self.jmne_db.all_ne_type()

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close all underlying database connections."""
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._kd2_db is not None:
            self._kd2_db.close()
            self._kd2_db = None
        if self._jmne_db is not None:
            self._jmne_db.close()
            self._jmne_db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __repr__(self) -> str:
        return (
            f"JamdictPeewee("
            f"db={self._db_path!r}, "
            f"xml={self._xml_path!r}, "
            f"kd2_db={self._kd2_db_path!r}, "
            f"jmne_db={self._jmne_db_path!r})"
        )
