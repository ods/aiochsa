from datetime import datetime
from decimal import Decimal

import sqlalchemy as sa


async def test_simple_round(conn, test_table):
    now = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
        'timestamp': now,
        'amount': Decimal('1.23'),
    }
    await conn.execute(
        test_table.insert()
            .values(values)
    )

    rows = await conn.fetch(test_table.select())
    row = await conn.fetchrow(test_table.select())
    assert rows == [row]
    assert dict(row) == values


async def test_defaults(conn, test_table):
    ts_before = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
    }
    await conn.execute(
        test_table.insert()
            .values(values.copy())
    )
    ts_after = datetime.utcnow().replace(microsecond=0)

    row = await conn.fetchrow(test_table.select())
    for field, value in values.items():
        assert row[field] == value
    assert ts_before <= row['timestamp'] <= ts_after
    assert row['amount'] == Decimal(0)


async def test_insert_multiple(conn, test_table):
    values = [
        {
            'id': i + 1,
            'name': f'test{i + 1}',
            'enum': 'TWO' if i % 2 else 'ONE',
        }
        for i in range(6)
    ]
    await conn.execute(
        test_table.insert()
            .values([d.copy() for d in values])
    )

    rows = await conn.fetch(
        sa.select([test_table.c.id])
            .where(test_table.c.enum == 'ONE')
            .order_by(test_table.c.id.desc())
    )
    assert [row[0] for row in rows] == [5, 3, 1]


async def test_insert_multiple_args(conn, test_table):
    values = [
        {'id': i + 1, 'name': f'test{i + 1}'}
        for i in range(3)
    ]
    await conn.execute(
        test_table.insert(), *[d.copy() for d in values],
    )

    rows = await conn.fetch(
        sa.select([test_table.c.id, test_table.c.name])
    )
    assert [dict(row) for row in rows] == values


async def test_join(conn, test_table):
    await conn.execute(
        test_table.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    test_alias = test_table.alias()
    rows = await conn.fetch (
        sa.select([test_table.c.id, test_alias.c.id])
            .select_from(
                test_table.join(
                    test_alias,
                    test_alias.c.id == test_table.c.id,
                )
            )
    )
    assert {tuple(row.values()) for row in rows} == {(1, 1), (2, 2), (3, 3)}
