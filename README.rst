aiochsa
=======

Clickhouse Python/asyncio library for use with SQLAlchemy core


Example
-------

.. code-block:: python

    import aiochsa
    import sqlalchemy as sa

    table = sa.Table(
        'test', sa.MetaData(),
        sa.Column('id', sa.Integer),
        sa.Column('name', sa.String),
    )

    async with aiochsa.connect('clickhouse://127.0.0.1:8123') as conn:
        await conn.execute(
            table.insert(),
            [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
        )
        rows = await conn.fetch(
            table.select()
        )


To add ``FINAL`` modifier use ``with_hint(table, 'FINAL')``
(see `SQLAlchemy docs for details <https://docs.sqlalchemy.org/en/13/core/selectable.html?highlight=with_hint#sqlalchemy.sql.expression.Select.with_hint>`_).

Configure logging to show SQL:

.. code-block:: python

    logging.getLogger('aiochsa.client.SQL').setLevel(logging.DEBUG)


Custom type converters
----------------------

Here is an example of installing converter for ClickHouse's ``DateTime`` type
that requires and returns timezone-aware Python's ``datetime`` object and
stores it as UTC:

.. code-block:: python

    from datetime import datetime
    import aiochsa
    from aiochsa.types import DateTimeUTCType, TypeRegistry

    types = TypeRegistry()
    types.register(DateTimeUTCType, ['DateTime'], datetime)
    conn = aiochsa.connect(dsn, types=types)


Change log
----------

See `CHANGELOG <https://github.com/ods/aiochsa/blob/master/CHANGELOG.rst>`_.
