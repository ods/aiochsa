from datetime import date, datetime
from decimal import Decimal
from enum import Enum

import pytest
import sqlalchemy as sa

import aiochsa


async def test_ddl(conn, table_test1):
    await conn.execute(sa.DDL(f'DROP TABLE {table_test1.name}'))


@pytest.mark.xfail(
    raises=TypeError,
    # Is the game worth the candle? For it compilation can't be separated from
    # execution.
    reason='Execution of default is not supported yet',
)
async def test_execute_default(conn, table_test1):
    ts_before = datetime.utcnow().replace(microsecond=0)
    now = await conn.fetchval(table_test1.c.timestamp.default)
    ts_after = datetime.utcnow().replace(microsecond=0)
    assert ts_before <= now <= ts_after


async def test_func(conn):
    ts_before = datetime.utcnow().replace(microsecond=0)
    now = await conn.fetchval(sa.func.now())
    ts_after = datetime.utcnow().replace(microsecond=0)
    assert ts_before <= now <= ts_after


async def test_func_params(conn):
    result = await conn.fetchval(
        sa.func.plus(sa.bindparam('a'), sa.bindparam('b'))
            .params({'a': 12, 'b': 23})
    )
    assert result == 35


async def test_func_params_args(conn):
    result = await conn.fetchval(
        sa.func.plus(sa.bindparam('a'), sa.bindparam('b')),
        {'a': 12, 'b': 23},
    )
    assert result == 35


async def test_simple_round(conn, table_test1):
    now = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
        'timestamp': now,
        'amount': Decimal('1.23'),
    }
    await conn.execute(
        table_test1.insert()
            .values(values)
    )

    rows = await conn.fetch(table_test1.select())
    row1 = await conn.fetchrow(table_test1.select())
    row2 = await conn.fetchrow(f'SELECT * from {table_test1.name}')
    assert rows == [row1]
    assert row1 == row2 == values


async def test_non_ascii(conn, table_test1):
    sample = 'зразок'
    await conn.execute(
        table_test1.insert()
            .values(id=1, name=sample)
    )
    name = await conn.fetchval(
        sa.select([table_test1.c.name])
            .where(table_test1.c.id == 1)
    )
    assert name == sample


async def test_enum(conn, table_test1):

    class EnumType(str, Enum):
        ONE = 'ONE'
        TWO = 'TWO'

    await conn.execute(
        table_test1.insert()
            .values(id=1, enum=EnumType.ONE)
    )
    value = await conn.fetchval(
        sa.select([table_test1.c.enum])
            .where(table_test1.c.id == 1)
    )
    assert value == EnumType.ONE


async def test_unsupported_type(conn):
    with pytest.raises(TypeError):
        await conn.fetchval(
            sa.select([sa.bindparam('a')])
                .params(a=...)
        )


@pytest.mark.xfail(reason='Feature is disabled in favour of speed')
async def test_defaults(conn, table_test1):
    ts_before = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
    }
    await conn.execute(
        table_test1.insert()
            .values(values)
    )
    ts_after = datetime.utcnow().replace(microsecond=0)

    row = await conn.fetchrow(table_test1.select())
    for field, value in values.items():
        assert row[field] == value
    assert ts_before <= row['timestamp'] <= ts_after
    assert row['amount'] == Decimal(0)


async def test_insert_multiple(conn, table_test1):
    values = [
        {
            'id': i + 1,
            'name': f'test{i + 1}',
            'enum': 'TWO' if i % 2 else 'ONE',
        }
        for i in range(6)
    ]
    await conn.execute(
        table_test1.insert()
            .values(values)
    )

    rows = await conn.fetch(
        sa.select([table_test1.c.id])
            .where(table_test1.c.enum == 'ONE')
            .order_by(table_test1.c.id.desc())
    )
    assert [item_id for (item_id,) in rows] == [5, 3, 1]


async def test_insert_multiple_args(conn, table_test1):
    values = [
        {'id': i + 1, 'name': f'test{i + 1}'}
        for i in range(3)
    ]
    await conn.execute(
        table_test1.insert(), *values,
    )

    rows = await conn.fetch(
        sa.select([table_test1.c.id, table_test1.c.name])
    )
    assert rows == values


async def test_insert_select(conn, table_test1, table_test2):
    values = [
        {'id': i + 1, 'name': f'test{i + 1}'}
        for i in range(3)
    ]
    await conn.execute(
        table_test1.insert(), *values,
    )

    await conn.execute(
        table_test2.insert()
            .from_select(
                [table_test2.c.num, table_test2.c.title],
                sa.select([table_test1.c.id, table_test1.c.name])
                    .where(table_test1.c.id > 1)
            )
    )
    rows = await conn.fetch(
        table_test2.select()
    )
    assert rows == [('test2', 2), ('test3', 3)]


@pytest.mark.xfail(reason='CTE is not supported by clickhouse-sqlalchemy')
async def test_insert_select_cte(conn, table_test2):
    await conn.execute(
        table_test2.insert(), {'title': 'test', 'num': 1},
    )

    max_num = (
        sa.select([sa.func.max(table_test2.c.num)])
            .cte()
    )

    await conn.execute(
        table_test2.insert()
            .from_select(
                [table_test2.c.num, table_test2.c.title],
                sa.select([max_num, table_test2.c.title])
            )
    )
    rows = await conn.fetch(
        table_test2.select()
    )
    assert rows == [('test', 1), ('test', 2)]


async def test_join(conn, table_test1):
    await conn.execute(
        table_test1.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    test_alias = table_test1.alias()
    rows = await conn.fetch (
        sa.select([table_test1.c.id, test_alias.c.id])
            .select_from(
                table_test1.join(
                    test_alias,
                    test_alias.c.id == table_test1.c.id,
                )
            )
    )
    assert {tuple(row) for row in rows} == {(1, 1), (2, 2), (3, 3)}


async def test_iterate(conn, table_test1):
    await conn.execute(
        table_test1.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows_agen = conn.iterate(
        sa.select([table_test1.c.id])
    )
    assert [item_id async for (item_id,) in rows_agen] == [1, 2, 3]


async def test_fetchval_empty(conn, table_test1):
    value = await conn.fetchval(
        sa.select([table_test1.c.id])
    )
    assert value is None


async def test_select_params(conn, table_test1):
    await conn.execute(
        table_test1.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows = await conn.fetch(
        sa.select([table_test1.c.id])
            .where(table_test1.c.id >= sa.bindparam('min_id'))
            .params(min_id=2)
    )
    assert [item_id for (item_id,) in rows] == [2, 3]


async def test_select_params_args(conn, table_test1):
    await conn.execute(
        table_test1.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows = await conn.fetch(
        sa.select([table_test1.c.id])
            .where(table_test1.c.id >= sa.bindparam('min_id')),
        {'min_id': 2},
    )
    assert [item_id for (item_id,) in rows] == [2, 3]


async def test_final_hint(conn, table_test3):
    await conn.execute(
        table_test3.insert(),
        *[{'key': 123, 'value': i} for i in range(10)]
    )
    rows = await conn.fetch(
        table_test3.select()
            .with_hint(table_test3, 'FINAL')
    )
    assert len(rows) == 1
    assert rows[0]['value'] == 45


async def test_aggregate_function(conn, table_test4):
    await conn.execute('INSERT INTO test4 SELECT 1, sumState(123)')
    value = await conn.fetchval(
        sa.select([table_test4.c.value])
    )
    assert isinstance(value, aiochsa.types.AggregateFunction)
    value = await conn.fetchval(
        sa.select([sa.func.sumMerge(table_test4.c.value)])
    )
    assert value == 123


async def test_nested_structures(conn):
    value = await conn.fetchval(
        r"SELECT ("
            r"1, "
            r"(2, NULL, 'a\nb\tc\0d\'', ['a', '\'']),"
            r"1.23, toDecimal32('1.23', 2), toDate('2000-01-01')"
        r")"
    )
    assert value == (
        1,
        (2, None, 'a\nb\tc\0d\'', ['a', '\'']),
        1.23, Decimal('1.23'), date(2000, 1, 1)
    )
