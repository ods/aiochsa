aiochsa
=======

An experimental wrapper around aiochclient_ to use it with SQLAlchemy

.. _aiochclient: https://github.com/maximdanilchenko/aiochclient

Example
-------

.. code-block:: python

    import aiochsa
    import sqlalchemy as sa

    table = sa.Table(
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
