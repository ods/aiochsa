from datetime import date, datetime, timezone
from decimal import Decimal
import itertools

import pytest
import sqlalchemy as sa
from clickhouse_sqlalchemy import types as t

import aiochsa
from aiochsa.types import TypeRegistry, DateTimeUTCType


def parametrized_id(value):
    if isinstance(value, type):
        # Types without parameters, e.g. `Int32` -> `Int32()`
        value = value()
    return repr(value)


@pytest.mark.parametrize(
    'sa_type,value',
    itertools.chain(*[
        list(itertools.product([sa_type], values)) for sa_type, values in
        [
            (t.String, ['', '\0', "'", '"', '\\', "\\'", 'зразок']),
            (t.String(16), ['', 'зразок']),

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
        ]
    ]),
    ids = parametrized_id,
)
async def test_encode_decode_round(conn, sa_type, value):
    result = await conn.fetchval(
        sa.select([sa.func.cast(value, sa_type)])
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
async def test_zero_dates(conn, sa_type, value):
    result = await conn.fetchval(
        sa.select([sa.func.cast(value, sa_type)])
    )
    assert result is None


@pytest.fixture
async def conn_utc(dsn):
    types = TypeRegistry()
    types.register(DateTimeUTCType, ch_types=['DateTime'], py_types=datetime)
    async with aiochsa.connect(dsn, types=types) as conn:
        yield conn


@pytest.mark.parametrize('value', [
    datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
    datetime.utcnow().replace(tzinfo=timezone.utc, microsecond=0),
])
async def test_datetime_utc(conn_utc, value):
    result = await conn_utc.fetchval(
        sa.func.toDateTime(value)
    )
    assert result == value


async def test_datetime_utc_pass_naive(conn_utc):
    with pytest.raises(ValueError):
        await conn_utc.fetchval(
            sa.func.toDateTime(datetime.now())
        )
