Change Log
==========

1.2.2 (2022-02-21)
------------------

* Temporarily restrict SQLAlchemy version <1.4


1.2.1 (2022-01-04)
------------------

* Fix handling of PCT-encoded parts in DSN
* Fix Pool explicit parameters precedence over ones from DSN


1.2.0 (2021-10-07)
------------------

* Fix DBException message parser for Clickhouse 21.9


1.1.0 (2021-03-19)
------------------

* Check and fix untyped definitions
* Mark package as typed


1.0.0 (2021-02-20)
------------------

* Switch to ``limit_by`` provided by new version of ``clickhouse_sqlalchemy``
  (incompatible interface)
* Fix type annotations


0.11.0 (2020-11-01)
-------------------

* Add support for ``DateTime`` with timezone


0.10.1 (2020-07-24)
-------------------

* Richer exception interface to simplify debugging huge inserts


0.10.0 (2020-06-17)
-------------------

* Add support for ``LIMIT BY`` clause
* Add support for parameters in DSN
* Add ``session_timeout`` parameter to ``connect``


0.9.2 (2020-03-23)
------------------

* Add ``timeout`` parameter to ``Pool.acquire()`` and ``Pool.release()``
  methods for interface compatibility with other drivers (like asyncpg)


0.9.1 (2020-03-19)
------------------

* Fix DBException message parser for Clickhouse 20.3
* Fix Decimal64 support for Clickhouse 20.1.2.4


0.9 (2020-02-10)
----------------

* Retry query if it failed due to broken connection or session state
* Wrap all protocol-related errors into ``ProtocolError``
* Log SQL queries (separate logger is used for flexibility)


0.8 (2020-01-22)
----------------

* Basic support for AggregateFunction with gentle reminder that you probably
  forget to use ``finalizeAggregation()`` or one of ``*Merge()`` functions
* ``aiochsa.__version__``


0.7 (2019-12-25)
----------------

* Add support for ``FINAL`` hint


0.6 (2019-12-09)
----------------

* Patch compiler from ``clickhouse_sqlalchemy`` in subclass instead of
  depending on fork (that caused problems when installing from PyPI)
* Add support for ``SimpleAggregateFunction()`` types
* Compatibility with Python 3.8


0.5 (2019-11-12)
----------------

* Pass INSERT values in JSONEachRow format instead of inlining (that makes
  driver about 10x times faster)
* Column defaults now are ignored
* Statement fragment in string representation of exception


0.4 (2019-10-29)
----------------

* Remove dependency on ``aiochclient``
* More careful cleanup of resources


0.3 (2019-10-23)
----------------

* Switch from TSVWithNamesAndTypes to JSONCompact
* Add support for custom type converters


0.2 (2019-09-17)
----------------

* Parse code, display text and stack trace from error responses
* Richer ``Record`` interface mimicking both dict and tuple (like in asyncpg)


0.1 (2019-09-16)
----------------

* Initial public release
