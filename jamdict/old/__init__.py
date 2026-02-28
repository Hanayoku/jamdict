# -*- coding: utf-8 -*-

"""
Legacy / old SQLite-backed implementations.

These modules use puchikarui as the database abstraction layer and have been
superseded by the peewee-backed implementation in util.py / jmdict_peewee.py
/ kanjidic2_peewee.py / jmnedict_peewee.py.

They are kept here for reference and for the parity test suite
(test_phase6.py) that compares old and new backends side-by-side.
"""

from .jmdict_sqlite import JMDictSQLite
from .jmnedict_sqlite import JMNEDictSQLite
from .kanjidic2_sqlite import KanjiDic2SQLite
from .util_old import (
    _JAMDICT_DATA_AVAILABLE,
    _MEMORY_MODE,
    IterLookupResult,
    JamdictSQLite,
    JMDictXML,
    JMNEDictXML,
    KanjiDic2XML,
    LookupResult,
)
from .util_old import (
    Jamdict as JamdictOld,
)

__all__ = [
    # SQLite schema classes
    "JMDictSQLite",
    "KanjiDic2SQLite",
    "JMNEDictSQLite",
    # util_old public symbols
    "JamdictOld",
    "JamdictSQLite",
    "LookupResult",
    "IterLookupResult",
    "JMDictXML",
    "JMNEDictXML",
    "KanjiDic2XML",
    # flags
    "_JAMDICT_DATA_AVAILABLE",
    "_MEMORY_MODE",
]
