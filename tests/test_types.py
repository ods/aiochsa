from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import enum
from ipaddress import IPv4Address, IPv6Address
import itertools
from random import randrange
from typing import Iterable, Tuple, Type, Union
import uuid

import pytest
import sqlalchemy as sa
from clickhouse_sqlalchemy import types as t

import aiochsa
from aiochsa.types import (
    ArrayType, BaseType, DateTimeUTCType, IntType, NullableType, ProxyType,
    StrType, TupleType, TypeRegistry,
)


def parametrized_id(value):
    if isinstance(value, type):
        # Types without parameters, e.g. `Int32` -> `Int32()`
        value = value()
    return repr(value)


MAX_IPV4 = 256**4 - 1
MAX_IPV6 = 256**16 - 1


class PyEnum(str, enum.Enum):
    FOO = 'FOO'
    BAR = 'BAR'
    BAZ = 'BAZ'


# Int version of enum is needed for definition in CAST only.

class CHEnum8(int, enum.Enum):
    FOO = -128
    BAR = 0
    BAZ = 127

class CHEnum16(int, enum.Enum):
    FOO = -32768
    BAR = 0
    BAZ = 32767


class CustomStr(str):
    pass


SaType = Union[sa.types.TypeEngine, Type[sa.types.TypeEngine]]


def combine_typed_rapameters(spec_seq: Iterable[Tuple[SaType, Iterable]]):
    return list( # Wrap into list to make it reusable (iterator is one-off)
        itertools.chain(*[
            list(itertools.product([sa_type], values))
            for sa_type, values in spec_seq
        ])
    )


TYPED_PARAMETERS = combine_typed_rapameters([
    (t.String, ['', '\0', "'", '"', '\\', "\\'", 'зразок', CustomStr('abc')]),
    (t.String(16), ['', 'зразок']),

    (t.Enum8(CHEnum8), list(PyEnum)),
    (t.Enum16(CHEnum16), list(PyEnum)),

    (t.Int8, [-128, 0, 127]),
    (t.UInt8, [0, 255]),
    (t.Int16, [-32768, 0, 32767]),
    (t.UInt16, [0, 65535]),
    (t.Int32, [-2147483648, 0, 2147483647]),
    (t.UInt32, [0, 4294967295]),
    (t.Int64, [-9223372036854775808, 0, 9223372036854775807]),
    (t.UInt64, [0, 18446744073709551615]),

    # `nan`, `inf`, `-inf` are returned as `null` in JSON formats
    # 0. and -0. are considered equal in Python
    (t.Float32, [0., -0., 1e-37, -1e-37, 1e38, -1e38]),
    (t.Float64, [0., -0., 1e-307, -1e-307, 1e308, -1e308]),

    (t.Decimal(9, 4), map(Decimal, [0, '1.2345', '-1.2345'])),
    (t.Decimal(18, 9), map(Decimal,
        [0, '1.234567891', '-1.234567891']
    )),
    (t.Decimal(38, 19), map(Decimal,
        [0, '1.234567891234567891', '-1.234567891234567891']
    )),

    # Start of the epoch is not supported by Clickhouse, at least one
    # second or day must be added
    (t.Date, [date(1970, 1, 2), date.today()]),
    (t.DateTime, [
        datetime(1970, 1, 1, 0, 0, 1),
        datetime.now().replace(microsecond=0)
    ]),

    (t.UUID, [uuid.uuid4()]),
    (t.IPv4, map(IPv4Address, [0, MAX_IPV4, randrange(1, MAX_IPV4)])),
    (t.IPv6, map(IPv6Address, [0, MAX_IPV6, randrange(1, MAX_IPV6)])),

    (t.Nullable(t.String), [None, '', 'abc']),
    (t.LowCardinality(t.String), ['', 'abc']),
    (t.LowCardinality(t.Nullable(t.String)), [None, '', 'abc']),
    (t.Array(t.String), [['foo', 'bar']]),

    # TODO Tests for `Tuple`, including deeply nested
])


@pytest.mark.parametrize(
    'sa_type,value',
    TYPED_PARAMETERS,
    ids = parametrized_id,
)
async def test_cast_round(conn, sa_type, value):
    # Select parameters go through `escape()` method of type
    result = await conn.fetchval(
        sa.select([sa.func.cast(value, sa_type)])
    )
    assert result == value


@pytest.mark.parametrize(
    'sa_type,value',
    TYPED_PARAMETERS,
    ids = parametrized_id,
)
async def test_insert_select_round(conn, table_for_type, sa_type, value):
    # Insert parameters go through `to_json()` method of type
    table = await table_for_type(sa_type)
    await conn.execute(
        table.insert(),
        {'value': value},
    )
    result = await conn.fetchval(
        table.select()
    )
    assert result == value


@pytest.mark.parametrize(
    'value',
    [
        '', '\0', "'", '"', '\\', "\\'", 'зразок',
        -9223372036854775808, 0, 9223372036854775807,
        0., -0., 1e-307, -1e-307, 1e308, -1e308,
        [1, 2, 3], ['a', 'b', 'c'],
        None,
        ('abc', 123, 2.34, None, [1, 2, 3], (1, ['a', 'b', 'c'])),
        [('abc', 123), ('bcd', 234)],
    ],
    ids = parametrized_id,
)
async def test_as_is_round(conn, value):
    result = await conn.fetchval(
        # Without `bindparam()` the value is handled by SQLAlchemy as column
        # expression and not passed through escape
        sa.select([sa.bindparam('a')])
            .params(a=value)
    )
    assert result == value


@pytest.mark.parametrize(
    'sa_type,value',
    [
        (t.Date, '0000-00-00'),
        (t.DateTime, '0000-00-00 00:00:00'),
    ],
    ids = parametrized_id,
)
async def test_zero_dates(clickhouse_version, conn, sa_type, value):
    if clickhouse_version >= (20, 7):
        pytest.skip('Feature is dropped in 20.7')
    result = await conn.fetchval(
        sa.select([sa.func.cast(value, sa_type)])
    )
    assert result is None


@pytest.mark.parametrize('tz_name,tz_offset', [
    ('UTC', 0),
    ('EST', -18_000),
    ('Europe/Moscow', 10_800),
])
async def test_timezones(conn, tz_name, tz_offset):
    dt = datetime(2020, 1, 1)
    result = await conn.fetchval(
        sa.func.toTimeZone(sa.func.toDateTime(dt), tz_name).select()
    )
    assert result.utcoffset().total_seconds() == tz_offset
    assert result.astimezone(timezone.utc).replace(tzinfo=None) == dt


@pytest.fixture
async def conn_utc(dsn):
    types = TypeRegistry()
    types.register(DateTimeUTCType, ['DateTime'], datetime)
    async with aiochsa.connect(dsn, types=types) as conn:
        yield conn


DATETIME_UTC_PARAMETERS = [
    datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
    datetime.utcnow().replace(tzinfo=timezone.utc, microsecond=0),
]


@pytest.mark.parametrize(
    'value',
    DATETIME_UTC_PARAMETERS,
    ids = parametrized_id,
)
async def test_datetime_utc_escape(conn_utc, value):
    # Select parameters go through `escape()` method of type
    result = await conn_utc.fetchval(
        sa.func.toDateTime(value)
    )
    assert result == value


@pytest.mark.parametrize(
    'value',
    DATETIME_UTC_PARAMETERS,
    ids = parametrized_id,
)
async def test_datetime_utc_insert_select(conn_utc, table_for_type, value):
    # Insert parameters go through `to_json()` method of type
    table = await table_for_type(sa.DateTime)
    await conn_utc.execute(
        table.insert(),
        {'value': value},
    )

    result = await conn_utc.fetchval(
        table.select()
    )
    assert result == value


async def test_datetime_utc_escape_naive(conn_utc):
    # Select parameters go through `escape()` method of type
    with pytest.raises(ValueError):
        await conn_utc.fetchval(
            sa.func.toDateTime(datetime.now())
        )


async def test_datetime_utc_insert_naive(conn_utc, table_for_type):
    # Insert parameters go through `to_json()` method of type
    table = await table_for_type(sa.DateTime)
    with pytest.raises(ValueError):
        await conn_utc.execute(
            table.insert(),
            {'value': datetime.now()},
        )


@pytest.mark.parametrize('tz_name,tz_offset', [
    ('UTC', 0),
    ('EST', -18_000),
    ('Europe/Moscow', 10_800),
])
async def test_timezones_with_utc(conn_utc, tz_name, tz_offset):
    dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    result = await conn_utc.fetchval(
        sa.func.toTimeZone(sa.func.toDateTime(dt, 'UTC'), tz_name).select()
    )
    assert result == dt


@pytest.mark.parametrize('value', [0, 4294967295])
async def test_simple_aggregate_function(conn, recreate_table_for_type, value):
    table_name = await recreate_table_for_type(
        'SimpleAggregateFunction(max, "UInt32")'
    )
    await conn.execute(f'INSERT INTO {table_name} VALUES ({value})')
    result = await conn.fetchval(f'SELECT value FROM {table_name}')
    assert result == value


# Parser tests would be meaningless if something is wrong with `__eq__`, so
# it's better to insure it works.

def _get_all_subclasses(cls):
    for subcls in cls.__subclasses__():
        yield subcls
        yield from _get_all_subclasses(subcls)

SIMPLE_TYPE_CLASSES = [
    cls for cls in _get_all_subclasses(BaseType)
    if not cls.__slots__ and not issubclass(cls, ProxyType)
]


@pytest.mark.parametrize(
    'type_class', SIMPLE_TYPE_CLASSES,
)
def test_eq_simple(type_class):
    type_obj = type_class()
    assert type_obj == type_class()
    assert all(
        type_obj != other_type_class()
        for other_type_class in SIMPLE_TYPE_CLASSES
        if other_type_class is not type_class
    )


@pytest.mark.parametrize(
    'type_class',
    [ArrayType, NullableType],
)
def test_eq_wrapping(type_class):
    type_obj = type_class(StrType())
    assert type_obj == type_class(StrType())

    assert type_obj != StrType()
    assert repr(type_obj) != repr(StrType())

    assert type_obj != type_class(IntType())
    assert repr(type_obj) != repr(type_class(IntType()))


def test_eq_tuple():
    type_obj = TupleType(StrType(), IntType())
    assert type_obj == TupleType(StrType(), IntType())

    assert type_obj != TupleType(StrType())
    assert repr(type_obj) != repr(TupleType(StrType()))
