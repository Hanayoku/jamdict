# -*- coding: utf-8 -*-

"""
Jamdict public APIs — peewee-backed implementation.

This module exposes the exact same public API as the original util.py
(now renamed util_old.py), but all database operations are routed through
the peewee-backed JamdictPeewee / JMDictDB / KanjiDic2DB / JMNEDictDB
classes instead of puchikarui.

Intentional behavioural differences from util_old.py
-----------------------------------------------------
* ``memory_mode`` is silently ignored — peewee's SqliteDatabase does not
  need to be pre-loaded into RAM to give good performance, and the old
  ``MemorySource`` helper is broken in recent puchikarui anyway.
* The internal ``reuse_ctx`` / ``__jm_ctx`` plumbing is gone.  Peewee
  handles connection reuse transparently per-instance.
* ``jmdict``, ``kd2``, ``jmnedict`` properties still exist but return the
  peewee-backed *DB objects rather than puchikarui Schema objects.
  They are intentionally kept to avoid breaking code that only reads the
  property to check for None (e.g. ``if jam.kd2 is not None``).
"""

# This code is a part of jamdict library: https://github.com/neocl/jamdict
# :copyright: (c) 2016 Le Tuan Anh <tuananh.ke@gmail.com>
# :license: MIT, see LICENSE for more details.

import logging
import os
import warnings
from collections import OrderedDict
from typing import Iterator, List, Optional, Sequence

from chirptext.deko import HIRAGANA, KATAKANA

from . import config
from .jmdict import JMDEntry, JMDictXMLParser
from .jmdict_peewee import JMDictDB
from .jmnedict_peewee import JMNEDictDB
from .kanjidic2 import Character, Kanjidic2XMLParser
from .kanjidic2_peewee import KanjiDic2DB
from .krad import KRad

# Keep this flag as True — the new backend does not need MemorySource but
# external code may import this symbol to check capability.
_MEMORY_MODE = True

try:
    import jamdict_data

    _JAMDICT_DATA_AVAILABLE = True
except Exception:
    _JAMDICT_DATA_AVAILABLE = False


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------


def getLogger():
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LookupResult  (identical to util_old.LookupResult)
# ---------------------------------------------------------------------------


class LookupResult:
    """Contain lookup results (words, Kanji characters, or named entities) from Jamdict.

    A typical jamdict lookup is like this:

    >>> jam = Jamdict()
    >>> result = jam.lookup('食べ%る')

    The command above returns a :any:`LookupResult` object which contains found words (:any:`entries`),
    kanji characters (:any:`chars`), and named entities (:any:`names`).
    """

    def __init__(self, entries, chars, names=None):
        self.__entries: Sequence[JMDEntry] = entries if entries else []
        self.__chars: Sequence[Character] = chars if chars else []
        self.__names: Sequence[JMDEntry] = names if names else []

    @property
    def entries(self) -> Sequence[JMDEntry]:
        """A list of words entries

        :returns: a list of :class:`JMDEntry <jamdict.jmdict.JMDEntry>` object
        :rtype: List[JMDEntry]
        """
        return self.__entries

    @entries.setter
    def entries(self, values: Sequence[JMDEntry]):
        self.__entries = values

    @property
    def chars(self) -> Sequence[Character]:
        """A list of found kanji characters

        :returns: a list of :class:`Character <jamdict.kanjidic2.Character>` object
        :rtype: Sequence[Character]
        """
        return self.__chars

    @chars.setter
    def chars(self, values: Sequence[Character]):
        self.__chars = values

    @property
    def names(self) -> Sequence[JMDEntry]:
        """A list of found named entities

        :returns: a list of :class:`JMDEntry <jamdict.jmdict.JMDEntry>` object
        :rtype: Sequence[JMDEntry]
        """
        return self.__names

    @names.setter
    def names(self, values: Sequence[JMDEntry]):
        self.__names = values

    def text(
        self,
        compact=True,
        entry_sep="。",
        separator=" | ",
        no_id=False,
        with_chars=True,
    ) -> str:
        """Generate a text string that contains all found words, characters, and named entities.

        :param compact: Make the output string more compact (fewer info, fewer whitespaces, etc.)
        :param no_id: Do not include jamdict's internal object IDs (for direct query via API)
        :param entry_sep: The text to separate entries
        :param with_chars: Include characters information
        :returns: A formatted string ready for display
        """
        output = []
        if self.entries:
            entry_txts = []
            for idx, e in enumerate(self.entries, start=1):
                entry_txt = e.text(compact=compact, separator=" ", no_id=no_id)
                entry_txts.append("#{}: {}".format(idx, entry_txt))
            output.append("[Entries]")
            output.append(entry_sep)
            output.append(entry_sep.join(entry_txts))
        elif not compact:
            output.append("No entries")
        if self.chars and with_chars:
            if compact:
                chars_txt = ", ".join(str(c) for c in self.chars)
            else:
                chars_txt = ", ".join(repr(c) for c in self.chars)
            if output:
                output.append(separator)
            output.append("[Chars]")
            output.append(entry_sep)
            output.append(chars_txt)
        if self.names:
            name_txts = []
            for idx, n in enumerate(self.names, start=1):
                name_txt = n.text(compact=compact, separator=" ", no_id=no_id)
                name_txts.append("#{}: {}".format(idx, name_txt))
            if output:
                output.append(separator)
            output.append("[Names]")
            output.append(entry_sep)
            output.append(entry_sep.join(name_txts))
        return "".join(output) if output else "Found nothing"

    def __repr__(self):
        return self.text(compact=True)

    def __str__(self):
        return self.text(compact=False)

    def to_json(self):
        warnings.warn(
            "to_json() is deprecated and will be removed in the next major release. Use to_dict() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.to_dict()

    def to_dict(self) -> dict:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "chars": [c.to_dict() for c in self.chars],
            "names": [n.to_dict() for n in self.names],
        }


# ---------------------------------------------------------------------------
# IterLookupResult  (identical to util_old.IterLookupResult)
# ---------------------------------------------------------------------------


class IterLookupResult:
    """Contain lookup results (words, Kanji characters, or named entities) from Jamdict.

    A typical jamdict lookup is like this:

    >>> res = jam.lookup_iter("花見")

    ``res`` is an :class:`IterLookupResult` object which contains iterators
    to scan through found words (``entries``), kanji characters (``chars``),
    and named entities (:any:`names`) one by one.

    >>> for word in res.entries:
    ...     print(word)  # do something with the word
    >>> for c in res.chars:
    ...     print(c)
    >>> for name in res.names:
    ...     print(name)
    """

    def __init__(self, entries, chars=None, names=None):
        self.__entries = entries if entries is not None else []
        self.__chars = chars if chars is not None else []
        self.__names = names if names is not None else []

    @property
    def entries(self):
        """Iterator for looping one by one through all found entries, can only be used once"""
        return self.__entries

    @property
    def chars(self):
        """Iterator for looping one by one through all found kanji characters, can only be used once"""
        return self.__chars

    @property
    def names(self):
        """Iterator for looping one by one through all found named entities, can only be used once"""
        return self.__names


# ---------------------------------------------------------------------------
# XML-backed helpers  (unchanged from util_old.py — no DB needed)
# ---------------------------------------------------------------------------


class JMDictXML:
    """JMDict API for looking up information in XML"""

    def __init__(self, entries):
        from collections import defaultdict as dd

        self.entries = entries
        self._seqmap = {}
        self._textmap = dd(set)
        for entry in self.entries:
            self._seqmap[entry.idseq] = entry
            for kn in entry.kana_forms:
                self._textmap[kn.text].add(entry)
            for kj in entry.kanji_forms:
                self._textmap[kj.text].add(entry)

    def __len__(self):
        return len(self.entries)

    def __getitem__(self, idx):
        return self.entries[idx]

    def lookup(self, a_query) -> Sequence[JMDEntry]:
        if a_query in self._textmap:
            return tuple(self._textmap[a_query])
        elif a_query.startswith("id#"):
            entry_id = a_query[3:]
            if entry_id in self._seqmap:
                return (self._seqmap[entry_id],)
        return ()

    @staticmethod
    def from_file(filename):
        parser = JMDictXMLParser()
        return JMDictXML(
            parser.parse_file(os.path.abspath(os.path.expanduser(filename)))
        )


class JMNEDictXML(JMDictXML):
    pass


class KanjiDic2XML:
    def __init__(self, kd2):
        self.kd2 = kd2
        self.char_map = {}
        for char in self.kd2:
            if char.literal in self.char_map:
                getLogger().warning(
                    "Duplicate character entry: {}".format(char.literal)
                )
            self.char_map[char.literal] = char

    def __len__(self):
        return len(self.kd2)

    def __getitem__(self, idx):
        return self.kd2[idx]

    def lookup(self, char):
        return self.char_map.get(char)

    @staticmethod
    def from_file(filename):
        parser = Kanjidic2XMLParser()
        return KanjiDic2XML(parser.parse_file(filename))


# ---------------------------------------------------------------------------
# Jamdict — same public API as util_old.Jamdict, backed by peewee
# ---------------------------------------------------------------------------


class Jamdict:
    """Main entry point to access all available dictionaries in jamdict.

    >>> from jamdict import Jamdict
    >>> jam = Jamdict()
    >>> result = jam.lookup('食べ%る')
    # print all word entries
    >>> for entry in result.entries:
    >>>     print(entry)
    # print all related characters
    >>> for c in result.chars:
    >>>     print(repr(c))

    To filter results by ``pos``, for example look for all "かえる" that are nouns, use:

    >>> result = jam.lookup("かえる", pos=["noun (common) (futsuumeishi)"])

    To search for named-entities by type, use the type string as query.
    For example to search for all "surname" use:

    >>> result = jam.lookup("surname")

    To find out which part-of-speeches or named-entities types are available in the
    dictionary, use :func:`Jamdict.all_pos <jamdict.util.Jamdict.all_pos>`
    and :func:`Jamdict.all_ne_type <jamdict.util.Jamdict.all_pos>`.

    Jamdict >= 0.1a10 support ``memory_mode`` keyword argument.
    With the peewee backend ``memory_mode`` is accepted for compatibility but
    is a no-op — the peewee implementation does not need pre-loading.

    When there is no suitable database available, Jamdict will try to use database
    from `jamdict-data <https://pypi.org/project/jamdict-data/>`_ package by default.
    """

    def __init__(
        self,
        db_file=None,
        kd2_file=None,
        jmd_xml_file=None,
        kd2_xml_file=None,
        auto_config=True,
        auto_expand=True,
        reuse_ctx=True,  # accepted for API compatibility, unused
        jmnedict_file=None,
        jmnedict_xml_file=None,
        memory_mode=False,  # accepted for API compatibility, no-op
        **kwargs,
    ):
        self.auto_expand = auto_expand
        # memory_mode is kept as an attribute for introspection but is a no-op
        self.__memory_mode = memory_mode

        # ---- resolve XML paths ------------------------------------------------
        self.jmd_xml_file = (
            jmd_xml_file
            if jmd_xml_file
            else config.get_file("JMDICT_XML")
            if auto_config
            else None
        )
        self.kd2_xml_file = (
            kd2_xml_file
            if kd2_xml_file
            else config.get_file("KD2_XML")
            if auto_config
            else None
        )
        self.jmnedict_xml_file = (
            jmnedict_xml_file
            if jmnedict_xml_file
            else config.get_file("JMNEDICT_XML")
            if auto_config
            else None
        )
        if auto_expand:
            if self.jmd_xml_file:
                self.jmd_xml_file = os.path.expanduser(self.jmd_xml_file)
            if self.kd2_xml_file:
                self.kd2_xml_file = os.path.expanduser(self.kd2_xml_file)
            if self.jmnedict_xml_file:
                self.jmnedict_xml_file = os.path.expanduser(self.jmnedict_xml_file)

        # ---- resolve DB paths ------------------------------------------------
        # db_file is the primary SQLite file for JMDict (and also KanjiDic2 /
        # JMNEDict when they share the same file, which is the default layout).
        self.db_file = (
            db_file
            if db_file
            else config.get_file("JAMDICT_DB")
            if auto_config
            else None
        )
        if not self.db_file or (
            self.db_file != ":memory:" and not os.path.isfile(self.db_file)
        ):
            if _JAMDICT_DATA_AVAILABLE:
                self.db_file = jamdict_data.JAMDICT_DB_PATH
            elif self.jmd_xml_file and os.path.isfile(self.jmd_xml_file):
                getLogger().warning(
                    "JAMDICT_DB could NOT be found. Searching will be extremely slow. "
                    "Please run `python3 -m jamdict import` first"
                )

        self.kd2_file = kd2_file if kd2_file else self.db_file if auto_config else None
        if not self.kd2_file or (
            self.kd2_file != ":memory:" and not os.path.isfile(self.kd2_file)
        ):
            if _JAMDICT_DATA_AVAILABLE:
                self.kd2_file = None
            elif self.kd2_xml_file and os.path.isfile(self.kd2_xml_file):
                getLogger().warning(
                    "Kanjidic2 database could NOT be found. Searching will be extremely slow. "
                    "Please run `python3 -m jamdict import` first"
                )

        self.jmnedict_file = (
            jmnedict_file if jmnedict_file else self.db_file if auto_config else None
        )
        if not self.jmnedict_file or (
            self.jmnedict_file != ":memory:" and not os.path.isfile(self.jmnedict_file)
        ):
            if _JAMDICT_DATA_AVAILABLE:
                self.jmnedict_file = None
            elif self.jmnedict_xml_file and os.path.isfile(self.jmnedict_xml_file):
                getLogger().warning(
                    "JMNE database could NOT be found. Searching will be extremely slow. "
                    "Please run `python3 -m jamdict import` first"
                )

        # ---- lazy-init database handles --------------------------------------
        self._db_peewee: Optional[JMDictDB] = None
        self._kd2_peewee: Optional[KanjiDic2DB] = None
        self._jmne_peewee: Optional[JMNEDictDB] = None

        # ---- lazy-init XML handles -------------------------------------------
        self._jmd_xml: Optional[JMDictXML] = None
        self._kd2_xml: Optional[KanjiDic2XML] = None
        self._jmne_xml: Optional[JMNEDictXML] = None

        # ---- krad map --------------------------------------------------------
        self.__krad_map: Optional[KRad] = None

    # ------------------------------------------------------------------
    # Properties — file paths with auto-expand
    # ------------------------------------------------------------------

    @property
    def db_file(self):
        return self.__db_file

    @db_file.setter
    def db_file(self, value):
        if self.auto_expand and value and value != ":memory:":
            self.__db_file = os.path.abspath(os.path.expanduser(value))
        else:
            self.__db_file = value

    @property
    def kd2_file(self):
        return self.__kd2_file

    @kd2_file.setter
    def kd2_file(self, value):
        if self.auto_expand and value and value != ":memory:":
            self.__kd2_file = os.path.abspath(os.path.expanduser(value))
        else:
            self.__kd2_file = value

    @property
    def jmnedict_file(self):
        return self.__jmnedict_file

    @jmnedict_file.setter
    def jmnedict_file(self, value):
        if self.auto_expand and value and value != ":memory:":
            self.__jmnedict_file = os.path.abspath(os.path.expanduser(value))
        else:
            self.__jmnedict_file = value

    @property
    def memory_mode(self) -> bool:
        """Accepted for API compatibility; always a no-op with the peewee backend."""
        return self.__memory_mode

    # ------------------------------------------------------------------
    # Properties — lazy DB handles
    # ------------------------------------------------------------------

    @property
    def ready(self) -> bool:
        """Check if the JMDict database is available."""
        return (
            self.db_file is not None
            and (self.db_file == ":memory:" or os.path.isfile(self.db_file))
            and self.jmdict is not None
        )

    @property
    def jmdict(self) -> Optional[JMDictDB]:
        """Lazily open the peewee-backed JMDict database."""
        if self._db_peewee is None and self.db_file:
            if self.db_file == ":memory:" or os.path.isfile(self.db_file):
                self._db_peewee = JMDictDB(self.db_file)
        return self._db_peewee

    @property
    def kd2(self) -> Optional[KanjiDic2DB]:
        """Lazily open the peewee-backed KanjiDic2 database."""
        if self._kd2_peewee is None:
            # When kd2_file == db_file (the default), they share the same
            # SQLite file — each peewee DB class manages its own table set,
            # so pointing both at the same file is safe.
            kd2_path = self.kd2_file if self.kd2_file else self.db_file
            if kd2_path and (kd2_path == ":memory:" or os.path.isfile(kd2_path)):
                self._kd2_peewee = KanjiDic2DB(kd2_path)
        return self._kd2_peewee

    @property
    def jmnedict(self) -> Optional[JMNEDictDB]:
        """Lazily open the peewee-backed JMNEDict database."""
        if self._jmne_peewee is None:
            jmne_path = self.jmnedict_file if self.jmnedict_file else self.db_file
            if jmne_path and (jmne_path == ":memory:" or os.path.isfile(jmne_path)):
                self._jmne_peewee = JMNEDictDB(jmne_path)
        return self._jmne_peewee

    # ------------------------------------------------------------------
    # Properties — lazy XML handles
    # ------------------------------------------------------------------

    @property
    def jmdict_xml(self) -> Optional[JMDictXML]:
        if not self._jmd_xml and self.jmd_xml_file:
            getLogger().info("Loading JMDict from XML file at %s", self.jmd_xml_file)
            self._jmd_xml = JMDictXML.from_file(self.jmd_xml_file)
            getLogger().info("Loaded JMdict entries: %d", len(self._jmd_xml))
        return self._jmd_xml

    @property
    def kd2_xml(self) -> Optional[KanjiDic2XML]:
        if not self._kd2_xml and self.kd2_xml_file:
            getLogger().info("Loading KanjiDic2 from XML file at %s", self.kd2_xml_file)
            self._kd2_xml = KanjiDic2XML.from_file(self.kd2_xml_file)
            getLogger().info("Loaded KanjiDic2 entries: %d", len(self._kd2_xml))
        return self._kd2_xml

    @property
    def jmne_xml(self) -> Optional[JMNEDictXML]:
        if not self._jmne_xml and self.jmnedict_xml_file:
            getLogger().info(
                "Loading JMnedict from XML file at %s", self.jmnedict_xml_file
            )
            self._jmne_xml = JMNEDictXML.from_file(self.jmnedict_xml_file)
            getLogger().info("Loaded JMnedict entries: %d", len(self._jmne_xml))
        return self._jmne_xml

    # ------------------------------------------------------------------
    # Properties — krad / radk
    # ------------------------------------------------------------------

    @property
    def krad(self):
        """Break a kanji down to writing components.

        >>> jam = Jamdict()
        >>> print(jam.krad['雲'])
        ['一', '雨', '二', '厶']
        """
        if not self.__krad_map:
            self.__krad_map = KRad()
        return self.__krad_map.krad

    @property
    def radk(self):
        """Find all kanji with a writing component.

        >>> jam = Jamdict()
        >>> print(jam.radk['鼎'])
        {'鏡', '鼒', '鼐', '鼎', '鼑'}
        """
        if not self.__krad_map:
            self.__krad_map = KRad()
        return self.__krad_map.radk

    # ------------------------------------------------------------------
    # Availability helpers
    # ------------------------------------------------------------------

    def has_kd2(self) -> bool:
        return (
            self.db_file is not None
            or self.kd2_file is not None
            or self.kd2_xml_file is not None
        )

    def has_jmne(self, ctx=None) -> bool:
        """Check if the current database has JMNEDict support."""
        if self.jmnedict is not None:
            meta = self.jmnedict.get_meta("jmnedict.version")
            return meta is not None and len(meta) > 0
        return False

    def is_available(self) -> bool:
        return (
            self.db_file is not None
            or self.jmd_xml_file is not None
            or self.kd2_file is not None
            or self.kd2_xml_file is not None
            or self.jmnedict_file is not None
            or self.jmnedict_xml_file is not None
        )

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def import_data(self) -> None:
        """Import JMDict, KanjiDic2, and JMNEDict data from XML into SQLite."""
        # ---- JMDict ----------------------------------------------------------
        if (
            self.jmdict is not None
            and self.jmd_xml_file
            and os.path.isfile(self.jmd_xml_file)
        ):
            getLogger().info("Importing JMDict data from %s", self.jmd_xml_file)
            parser = JMDictXMLParser()
            entries = parser.parse_file(
                os.path.abspath(os.path.expanduser(self.jmd_xml_file))
            )
            self.jmdict.insert_entries(entries)
            getLogger().info("JMDict import complete (%d entries)", len(entries))
        else:
            getLogger().warning("JMDict XML data is not available — skipped!")

        # ---- KanjiDic2 -------------------------------------------------------
        if self.kd2_xml_file and os.path.isfile(self.kd2_xml_file):
            getLogger().info("Importing KanjiDic2 data from %s", self.kd2_xml_file)
            parser = Kanjidic2XMLParser()
            kd2 = parser.parse_file(
                os.path.abspath(os.path.expanduser(self.kd2_xml_file))
            )
            # Determine which KanjiDic2DB to write into.
            # When kd2_file == db_file, they share the file but KanjiDic2DB
            # manages its own table set, so we open a separate KanjiDic2DB
            # pointing at the same path.
            kd2_path = self.kd2_file if self.kd2_file else self.db_file
            if kd2_path:
                kd2_db = self.kd2  # lazy-open
                if kd2_db is not None:
                    kd2_db.update_kd2_meta(
                        kd2.file_version,
                        kd2.database_version,
                        kd2.date_of_creation,
                    )
                    kd2_db.insert_chars(kd2.characters)
                    getLogger().info(
                        "KanjiDic2 import complete (%d chars)", len(kd2.characters)
                    )
                else:
                    getLogger().warning("KanjiDic2 DB could not be opened — skipped!")
            else:
                getLogger().warning(
                    "KanjiDic2 DB path could not be determined — skipped!"
                )
        else:
            getLogger().warning("KanjiDic2 XML data is not available — skipped!")

        # ---- JMNEDict --------------------------------------------------------
        if self.jmnedict_xml_file and os.path.isfile(self.jmnedict_xml_file):
            getLogger().info("Importing JMNEDict data from %s", self.jmnedict_xml_file)
            # JMNEDictXML uses the same parser infrastructure as JMDict
            jmne_xml = JMNEDictXML.from_file(self.jmnedict_xml_file)
            ne_entries = list(jmne_xml)
            jmne_path = self.jmnedict_file if self.jmnedict_file else self.db_file
            if jmne_path:
                jmne_db = self.jmnedict  # lazy-open
                if jmne_db is not None:
                    jmne_db.insert_entries(ne_entries)
                    getLogger().info(
                        "JMNEDict import complete (%d entries)", len(ne_entries)
                    )
                else:
                    getLogger().warning("JMNEDict DB could not be opened — skipped!")
            else:
                getLogger().warning(
                    "JMNEDict DB path could not be determined — skipped!"
                )
        else:
            getLogger().warning("JMNEDict XML data is not available — skipped!")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_ne(self, idseq) -> Optional[JMDEntry]:
        """Get a named entity by idseq from JMNEDict."""
        if self.jmnedict is not None:
            return self.jmnedict.get_ne(idseq)
        elif self.jmnedict_xml_file:
            return self.jmne_xml.lookup(idseq)
        else:
            raise LookupError("There is no JMnedict data source available")

    def get_char(self, literal, ctx=None) -> Optional[Character]:
        """Get a kanji character by literal from KanjiDic2."""
        if self.kd2 is not None:
            return self.kd2.get_char(literal)
        elif self.kd2_xml:
            return self.kd2_xml.lookup(literal)
        else:
            raise LookupError("There is no KanjiDic2 data source available")

    def get_entry(self, idseq) -> Optional[JMDEntry]:
        """Get a JMDict entry by idseq."""
        if self.jmdict is not None:
            return self.jmdict.get_entry(idseq)
        elif self.jmdict_xml:
            return self.jmdict_xml.lookup(idseq)[0]
        else:
            raise LookupError("There is no backend data available")

    def all_pos(self, ctx=None) -> List[str]:
        """Return all available part-of-speech tags.

        :returns: A list of part-of-speeches (a list of strings)
        """
        if self.jmdict is not None:
            return self.jmdict.all_pos()
        return []

    def all_ne_type(self, ctx=None) -> List[str]:
        """Return all available named-entity type tags.

        :returns: A list of named-entity types (a list of strings)
        """
        if self.jmnedict is not None:
            return self.jmnedict.all_ne_type()
        return []

    # ------------------------------------------------------------------
    # Main lookup
    # ------------------------------------------------------------------

    def lookup(
        self,
        query,
        strict_lookup=False,
        lookup_chars=True,
        ctx=None,  # accepted for API compatibility, unused
        lookup_ne=True,
        pos=None,
        **kwargs,
    ) -> LookupResult:
        """Search words, characters, and named entities.

        Keyword arguments:

        :param query: Text to query, may contain wildcard characters.
            Use ``%`` to match any number of characters and ``_`` for exactly one.
        :param strict_lookup: Only look up the kanji characters literally present
            in *query* (i.e. do not expand characters from entry kanji forms).
        :type strict_lookup: bool
        :param lookup_chars: Set to ``False`` to disable character lookup.
        :type lookup_chars: bool
        :param pos: Filter words by part-of-speeches.
        :type pos: list of strings
        :param lookup_ne: Set to ``False`` to disable name-entity lookup.
        :type lookup_ne: bool
        :returns: A :class:`LookupResult` object.
        :rtype: LookupResult

        >>> jam = Jamdict()
        >>> results = jam.lookup('食べ%る')
        """
        if not self.is_available():
            raise LookupError("There is no backend data available")
        if (not query or query == "%") and not pos:
            raise ValueError("Query and POS filter cannot be both empty")

        # ---- word entries ----------------------------------------------------
        entries = []
        if self.jmdict is not None:
            entries = self.jmdict.search(query, pos=pos)
        elif self.jmdict_xml:
            entries = list(self.jmdict_xml.lookup(query))

        # ---- kanji characters ------------------------------------------------
        chars = []
        if lookup_chars and self.has_kd2():
            chars_to_search = OrderedDict({c: c for c in query})
            if not strict_lookup and entries:
                for e in entries:
                    for k in e.kanji_forms:
                        for c in k.text:
                            if c not in HIRAGANA and c not in KATAKANA:
                                chars_to_search[c] = c
            for c in chars_to_search:
                result = self.get_char(c)
                if result is not None:
                    chars.append(result)

        # ---- named entities --------------------------------------------------
        names = []
        if lookup_ne and self.has_jmne():
            names = self.jmnedict.search_ne(query)

        return LookupResult(entries, chars, names)

    def lookup_iter(
        self,
        query,
        strict_lookup=False,
        lookup_chars=True,
        lookup_ne=True,
        ctx=None,  # accepted for API compatibility, unused
        pos=None,
        **kwargs,
    ) -> IterLookupResult:
        """Search for words, characters, and named entities iteratively.

        An :class:`IterLookupResult` object will be returned.
        ``res.entries``, ``res.chars``, ``res.names`` are iterators and each
        can only be looped through once.

        >>> res = jam.lookup_iter("花見")
        >>> for word in res.entries:
        ...     print(word)
        >>> for c in res.chars:
        ...     print(c)
        >>> for name in res.names:
        ...     print(name)

        :param query: Text to query, may contain wildcard characters.
        :param strict_lookup: Only use characters literally in *query* for char lookup.
        :type strict_lookup: bool
        :param lookup_chars: Set to ``False`` to disable character lookup.
        :type lookup_chars: bool
        :param pos: Filter words by part-of-speeches.
        :type pos: list of strings
        :param lookup_ne: Set to ``False`` to disable name-entity lookup.
        :type lookup_ne: bool
        :returns: An :class:`IterLookupResult` object.
        :rtype: IterLookupResult
        """
        if not self.is_available():
            raise LookupError("There is no backend data available")
        if (not query or query == "%") and not pos:
            raise ValueError("Query and POS filter cannot be both empty")

        # ---- word entries (iterator) -----------------------------------------
        entries = None
        if self.jmdict is not None:
            entries = self.jmdict.search_iter(query, pos=pos)

        # ---- kanji characters (iterator) ------------------------------------
        chars = None
        if lookup_chars and self.has_kd2() and self.kd2 is not None:
            chars_to_search = [
                c for c in query if c not in HIRAGANA and c not in KATAKANA
            ]
            chars = self.kd2.search_chars_iter(chars_to_search)

        # ---- named entities (iterator) ---------------------------------------
        names = None
        if lookup_ne and self.has_jmne():
            names = self.jmnedict.search_ne_iter(query)

        return IterLookupResult(entries, chars, names)

    # ------------------------------------------------------------------
    # Resource management
    # ------------------------------------------------------------------

    def __del__(self):
        self._close_dbs()

    def _close_dbs(self):
        try:
            if self._db_peewee is not None:
                self._db_peewee.close()
                self._db_peewee = None
        except Exception:
            pass
        try:
            if self._kd2_peewee is not None:
                self._kd2_peewee.close()
                self._kd2_peewee = None
        except Exception:
            pass
        try:
            if self._jmne_peewee is not None:
                self._jmne_peewee.close()
                self._jmne_peewee = None
        except Exception:
            pass
