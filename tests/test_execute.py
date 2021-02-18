from datetime import date, datetime
from decimal import Decimal
from enum import Enum

import pytest
import sqlalchemy as sa

import aiochsa

async def test_ddl(conn, table_test):
    await conn.execute(sa.DDL(f'DROP TABLE {table_test.name}'))


@pytest.mark.xfail(
    raises=TypeError,
    # Is the game worth the candle? For it compilation can't be separated from
    # execution.
    reason='Execution of default is not supported yet',
)
async def test_execute_default(conn, table_test):
    ts_before = datetime.utcnow().replace(microsecond=0)
    now = await conn.fetchval(table_test.c.timestamp.default)
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


async def test_simple_round(conn, table_test):
    now = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
        'timestamp': now,
        'amount': Decimal('1.23'),
    }
    await conn.execute(
        table_test.insert()
            .values(values)
    )

    rows = await conn.fetch(table_test.select())
    row1 = await conn.fetchrow(table_test.select())
    row2 = await conn.fetchrow(f'SELECT * from {table_test.name}')
    assert rows == [row1]
    assert row1 == row2 == values


async def test_non_ascii(conn, table_test, any_select):
    sample = 'зразок'
    await conn.execute(
        table_test.insert()
            .values(id=1, name=sample)
    )
    name = await conn.fetchval(
        any_select([table_test.c.name])
            .where(table_test.c.id == 1)
    )
    assert name == sample


async def test_enum(conn, table_test, any_select):

    class EnumType(str, Enum):
        ONE = 'ONE'
        TWO = 'TWO'

    await conn.execute(
        table_test.insert()
            .values(id=1, enum=EnumType.ONE)
    )
    value = await conn.fetchval(
        any_select([table_test.c.enum])
            .where(table_test.c.id == 1)
    )
    assert value == EnumType.ONE


async def test_unsupported_type(conn, any_select):
    with pytest.raises(TypeError):
        await conn.fetchval(
            any_select([sa.bindparam('a')])
                .params(a=...)
        )


@pytest.mark.xfail(reason='Feature is disabled in favour of speed')
async def test_defaults(conn, table_test):
    ts_before = datetime.utcnow().replace(microsecond=0)
    values = {
        'id': 1,
        'enum': 'ONE',
        'name': 'test',
    }
    await conn.execute(
        table_test.insert()
            .values(values)
    )
    ts_after = datetime.utcnow().replace(microsecond=0)

    row = await conn.fetchrow(table_test.select())
    for field, value in values.items():
        assert row[field] == value
    assert ts_before <= row['timestamp'] <= ts_after
    assert row['amount'] == Decimal(0)


async def test_insert_multiple(conn, table_test, any_select):
    values = [
        {
            'id': i + 1,
            'name': f'test{i + 1}',
            'enum': 'TWO' if i % 2 else 'ONE',
        }
        for i in range(6)
    ]
    await conn.execute(
        table_test.insert()
            .values(values)
    )

    rows = await conn.fetch(
        any_select([table_test.c.id])
            .where(table_test.c.enum == 'ONE')
            .order_by(table_test.c.id.desc())
    )
    assert [item_id for (item_id,) in rows] == [5, 3, 1]


async def test_insert_multiple_args(conn, table_test, any_select):
    values = [
        {'id': i + 1, 'name': f'test{i + 1}'}
        for i in range(3)
    ]
    await conn.execute(
        table_test.insert(), *values,
    )

    rows = await conn.fetch(
        any_select([table_test.c.id, table_test.c.name])
    )
    assert rows == values


async def test_insert_select(conn, table_test, table_mt, any_select):
    values = [
        {'id': i + 1, 'name': f'test{i + 1}'}
        for i in range(3)
    ]
    await conn.execute(
        table_test.insert(), *values,
    )

    await conn.execute(
        table_mt.insert()
            .from_select(
                [table_mt.c.num, table_mt.c.title],
                any_select([table_test.c.id, table_test.c.name])
                    .where(table_test.c.id > 1)
            )
    )
    rows = await conn.fetch(
        table_mt.select()
    )
    assert rows == [(2, 'test2'), (3, 'test3')]


@pytest.mark.xfail(reason='CTE is not supported by clickhouse-sqlalchemy')
async def test_insert_select_cte(conn, table_mt, any_select):
    await conn.execute(
        table_mt.insert(), {'title': 'test', 'num': 1},
    )

    max_num = (
        any_select([sa.func.max(table_mt.c.num)])
            .cte()
    )

    await conn.execute(
        table_mt.insert()
            .from_select(
                [table_mt.c.num, table_mt.c.title],
                any_select([max_num, table_mt.c.title])
            )
    )
    rows = await conn.fetch(
        table_mt.select()
    )
    assert rows == [('test', 1), ('test', 2)]


async def test_join(conn, table_test, any_select):
    await conn.execute(
        table_test.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    test_alias = table_test.alias()
    rows = await conn.fetch (
        any_select([table_test.c.id, test_alias.c.id])
            .select_from(
                table_test.join(
                    test_alias,
                    test_alias.c.id == table_test.c.id,
                )
            )
    )
    assert {tuple(row) for row in rows} == {(1, 1), (2, 2), (3, 3)}


async def test_iterate(conn, table_test, any_select):
    await conn.execute(
        table_test.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows_agen = conn.iterate(
        any_select([table_test.c.id])
    )
    assert [item_id async for (item_id,) in rows_agen] == [1, 2, 3]


async def test_fetchval_empty(conn, table_test, any_select):
    value = await conn.fetchval(
        any_select([table_test.c.id])
    )
    assert value is None


async def test_select_params(conn, table_test, any_select):
    await conn.execute(
        table_test.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows = await conn.fetch(
        any_select([table_test.c.id])
            .where(table_test.c.id >= sa.bindparam('min_id'))
            .params(min_id=2)
    )
    assert [item_id for (item_id,) in rows] == [2, 3]


async def test_select_params_args(conn, table_test, any_select):
    await conn.execute(
        table_test.insert(),
        *[
            {'id': i + 1, 'name': f'test{i + 1}'}
            for i in range(3)
        ],
    )

    rows = await conn.fetch(
        any_select([table_test.c.id])
            .where(table_test.c.id >= sa.bindparam('min_id')),
        {'min_id': 2},
    )
    assert [item_id for (item_id,) in rows] == [2, 3]


async def test_final_hint(conn, table_smt):
    await conn.execute(
        table_smt.insert(),
        *[{'key': 123, 'value': i} for i in range(10)]
    )
    rows = await conn.fetch(
        table_smt.select()
            .with_hint(table_smt, 'FINAL')
    )
    assert len(rows) == 1
    assert rows[0]['value'] == 45


@pytest.mark.parametrize('alised', [False, True])
async def test_join_final_hint(
    conn, table_test, table_smt, alised, any_select,
):
    await conn.execute(
        table_test.insert(),
        {'id': 12, 'name': 'name12'},
        {'id': 23, 'name': 'name23'},
    )
    await conn.execute(
        table_smt.insert(),
        *[{'key': 12, 'value': i} for i in range(10)]
    )

    if alised:
        table_smt = table_smt.alias()
    query = (
        any_select([table_test.c.name, table_smt.c.value])
            .select_from(
                table_smt.join(
                    table_test,
                    table_test.c.id == sa.func.toUInt64(table_smt.c.key),
                )
            )
            .with_hint(table_smt, 'FINAL')
    )
    rows = await conn.fetch(query)
    assert len(rows) == 1
    assert rows[0]['name'] == 'name12'


async def test_aggregate_function(conn, table_amt, any_select):
    await conn.execute('INSERT INTO test_amt SELECT 1, sumState(123)')
    value = await conn.fetchval(
        any_select([table_amt.c.value])
    )
    assert isinstance(value, aiochsa.types.AggregateFunction)
    value = await conn.fetchval(
        any_select([sa.func.sumMerge(table_amt.c.value)])
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


async def test_limit_by_with_order_by(conn, table_mt):
    await conn.execute(
        table_mt.insert(),
        [
            {'num': 1, 'title': '13'},
            {'num': 1, 'title': '12'},
            {'num': 1, 'title': '11'},
            {'num': 2, 'title': '21'},
            {'num': 2, 'title': '22'},
        ],
    )

    query = (
        aiochsa.select([table_mt.c.title])
            .order_by(table_mt.c.num, table_mt.c.title)
    )
    rows = await conn.fetch(
        query.limit_by([table_mt.c.num], limit=2)
    )
    assert [title for (title,) in rows] == ['11', '12', '21', '22']

    rows = await conn.fetch(
        query.limit_by([table_mt.c.num], offset=1, limit=1)
    )
    assert [title for (title,) in rows] == ['12', '22']


async def test_limit_by_without_order_by(conn, table_smt):
    await conn.execute(
        table_smt.insert(),
        [
            {'key': 1, 'value': 13},
            {'key': 1, 'value': 12},
            {'key': 1, 'value': 11},
            {'key': 2, 'value': 21},
            {'key': 2, 'value': 22},
        ],
    )
    rows = await conn.fetch(
        aiochsa.select([table_smt.c.key])
            .limit_by([table_smt.c.key], limit=1)
    )
    assert {value for (value,) in rows} == {1, 2}
