import pytest

from aiochsa.parser import parse_type
from aiochsa import types as t


@pytest.mark.parametrize(
    'type_str,type_obj',
    [
        ('String', t.StrType()),
        ("Enum8('' = -128, 'a' = 0, '\t\n\0\\\'' = 127)", t.StrType()),
        ("Enum16('a' = 1)", t.StrType()),
        ('FixedString(8)', t.StrStripZerosType()),
        *[
            (ts, t.IntType()) for ts in [
                'UInt8', 'UInt16', 'UInt32', 'UInt64',
                'Int8', 'Int16', 'Int32', 'Int64',
            ]
        ],
        *[
            (ts, t.FloatType()) for ts in ['Float32', 'Float64']
        ],
        ('Decimal(18, 9)', t.DecimalType()),
        ('DateTime', t.DateTimeType()),
        ('Date', t.DateType()),
        ('UUID', t.UUIDType()),
        ('IPv4', t.IPv4Type()),
        ('IPv6', t.IPv6Type()),
        ('Nothing', t.NothingType()),
        ('Nullable(Nothing)', t.NullableType(t.NothingType())),
        ('LowCardinality(String)', t.LowCardinalityType(t.StrType())),
        (
            'LowCardinality(Nullable(String))',
            t.LowCardinalityType(t.NullableType(t.StrType()))
        ),
        ('Tuple(Int8, String)', t.TupleType(t.IntType(), t.StrType())),
        ('Array(String)', t.ArrayType(t.StrType())),
        (
            'Tuple('
                'UInt8, '
                'Array('
                    'Nullable(FixedString(2))'
                '), '
                'Array('
                    "Tuple(Decimal(9, 2), Enum16('' = -128))"
                '), '
                'Tuple('
                    "Nullable(Enum8('' = -128, 'a' = 1, '\t\n\0\\\'' = 2)), "
                    'Nullable(Nothing)'
                ')'
            ')',
            t.TupleType(
                t.IntType(),
                t.ArrayType(
                    t.NullableType(t.StrStripZerosType())
                ),
                t.ArrayType(
                    t.TupleType(t.DecimalType(), t.StrType()),
                ),
                t.TupleType(
                    t.NullableType(t.StrType()),
                    t.NullableType(t.NothingType()),
                ),
            )
        )
    ],
)
async def test_parse_type(type_str, type_obj):
    types = t.TypeRegistry()
    result = parse_type(types, type_str)
    assert result == type_obj
