from datetime import date, datetime, timezone
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import (
    Any, Callable, Generic, Iterable, List, Optional, Type, TypeVar, Union,
)
from uuid import UUID

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo


NoneType = type(None)
PyType = TypeVar('PyType')
JsonType = TypeVar('JsonType', NoneType, int, float, Decimal, str, list)


class BaseType(Generic[PyType, JsonType]):
    __slots__ = ()

    py_type: Optional[Type[PyType]] = None

    @classmethod
    def escape(cls, value: PyType, escape: Callable) -> str:
        return str(value)

    def from_json(self, value: JsonType) -> PyType:
        return self.py_type(value)

    @classmethod
    def to_json(cls, value: PyType, to_json: Callable) -> JsonType:
        return value

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(
                getattr(self, slot) == getattr(other, slot)
                for slot in self.__slots__
            )
        )

    def __repr__(self):
        return '{}({})'.format(
            type(self).__name__,
            ', '.join(repr(getattr(self, arg)) for arg in self.__slots__)
        )


class StrType(BaseType):
    py_type = str

    def __init__(self, *params):
        pass

    @classmethod
    def escape(cls, value: str, escape=None) -> str:
        value = value.replace('\\', '\\\\').replace("'", "\\'")
        return f"'{value}'"


class StrStripZerosType(StrType):

    def from_json(self, value: str) -> str:
        return value.rstrip('\0')


class IntType(BaseType):
    py_type = int


class FloatType(BaseType):
    py_type = float


class DecimalType(BaseType):
    py_type = Decimal

    def __init__(self, *params):
        pass

    @classmethod
    def escape(cls, value: PyType, escape: Callable) -> str:
        return f"'{value}'"

    @classmethod
    def to_json(cls, value: PyType, to_json: Callable) -> JsonType:
        # Clickhouse requires serializing it without quotes, so we use
        # `simplejson.dumps(..., use_decimal=True)`
        return value


class DateType(BaseType):
    py_type = date

    @classmethod
    def escape(cls, value: date, escape=None) -> str:
        return f"'{value.isoformat()}'"

    @classmethod
    def to_json(cls, value: date, to_json: Callable) -> JsonType:
        return value.isoformat()

    def from_json(self, value: str) -> Optional[date]:
        if value == '0000-00-00':
            return None
        return date.fromisoformat(value)


class DateTimeType(BaseType):
    py_type = datetime

    __slots__ = ('_tzinfo',)

    def __init__(self, tz_name=None):
        if tz_name is None:
            self._tzinfo = None
        else:
            self._tzinfo = zoneinfo.ZoneInfo(tz_name)

    @classmethod
    def escape(cls, value: datetime, escape=None) -> str:
        value = value.replace(tzinfo=None, microsecond=0)
        return f"'{value.isoformat()}'"

    @classmethod
    def to_json(cls, value: datetime, to_json: Callable) -> JsonType:
        value = value.replace(tzinfo=None, microsecond=0)
        return value.isoformat()

    def from_json(self, value: str) -> Optional[datetime]:
        if value == '0000-00-00 00:00:00':
            return None
        result = datetime.fromisoformat(value)
        if self._tzinfo is not None:
            result = result.replace(tzinfo=self._tzinfo)
        return result


class DateTimeUTCType(DateTimeType):

    @classmethod
    def escape(cls, value: datetime, escape=None) -> str:
        if value.utcoffset() is None:
            raise ValueError(
                'Got naive datetime while timezone-aware is expected'
            )
        value = (
            value.astimezone(timezone.utc)
                .replace(tzinfo=None, microsecond=0)
        )
        return f"'{value.isoformat()}'"

    @classmethod
    def to_json(cls, value: datetime, to_json: Callable) -> JsonType:
        if value.utcoffset() is None:
            raise ValueError(
                'Got naive datetime while timezone-aware is expected'
            )
        value = (
            value.astimezone(timezone.utc)
                .replace(tzinfo=None, microsecond=0)
        )
        return value.isoformat()

    def from_json(self, value: str) -> datetime:
        result = datetime.fromisoformat(value)
        if self._tzinfo is None:
            return result.replace(tzinfo=timezone.utc)
        else:
            return result.replace(tzinfo=self._tzinfo).astimezone(timezone.utc)


class UUIDType(BaseType):
    py_type = UUID

    @classmethod
    def escape(cls, value: UUID, escape=None) -> str:
        return f"'{value}'"

    @classmethod
    def to_json(cls, value: UUID, to_json: Callable) -> JsonType:
        return str(value)


class IPv4Type(BaseType):
    py_type = IPv4Address

    @classmethod
    def to_json(cls, value: IPv4Address, to_json: Callable) -> JsonType:
        return str(value)


class IPv6Type(BaseType):
    py_type = IPv6Address

    @classmethod
    def to_json(cls, value: IPv6Address, to_json: Callable) -> JsonType:
        return str(value)


class NothingType(BaseType):
    py_type = NoneType

    def escape(self, value: NoneType, escape=None) -> str:
        return 'NULL'

    def from_json(self, value: NoneType) -> None:
        # Actually it's never called
        return None # pragma: nocover


class TupleType(BaseType):
    __slots__ = ('_item_types',)

    py_type = tuple

    def __init__(self, *item_types):
        self._item_types = item_types

    @classmethod
    def escape(cls, value: tuple, escape=None) -> str:
        return '({})'.format(
            ','.join(escape(v) for v in value)
        )

    @classmethod
    def to_json(cls, value: tuple, to_json: Callable) -> JsonType:
        return [to_json(v) for v in value]

    def from_json(self, value: List[JsonType]) -> tuple:
        assert len(self._item_types) == len(value)
        return tuple(
            t.from_json(v) for t, v in zip(self._item_types, value)
        )


class ArrayType(BaseType):
    __slots__ = ('_item_type',)

    py_type = list

    def __init__(self, item_type):
        self._item_type = item_type

    @classmethod
    def escape(cls, value: list, escape) -> str:
        return '[{}]'.format(
            ','.join(escape(v) for v in value)
        )

    @classmethod
    def to_json(cls, value: list, to_json: Callable) -> JsonType:
        return [to_json(v) for v in value]

    def from_json(self, value: List[JsonType]) -> list:
        return [self._item_type.from_json(v) for v in value]


class ProxyType(BaseType):

    def __new__(cls, item_type):
        return item_type

    @classmethod
    def escape(cls, value: PyType, escape=None) -> str:
        raise RuntimeError('Must be never called')  # pragma: nocover

    @classmethod
    def to_json(cls, value: PyType, to_json: Callable) -> JsonType:
        raise RuntimeError('Must be never called')  # pragma: nocover


class NullableType(BaseType):
    __slots__ = ('_item_type',)

    def __init__(self, item_type):
        self._item_type = item_type

    @classmethod
    def escape(cls, value: PyType, escape=None) -> str:
        raise RuntimeError('Must be never called')  # pragma: nocover

    @classmethod
    def to_json(cls, value: PyType, to_json: Callable) -> JsonType:
        raise RuntimeError('Must be never called')  # pragma: nocover

    def from_json(self, value: JsonType) -> Any:
        if value is None:
            return
        return self._item_type.from_json(value)


class AggregateFunction:

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f'<AggregateFunction (did you forget to finalize aggregation?)>'


class AggregateFunctionType(BaseType):
    __slots__ = ('_item_type',)

    def __init__(self, item_type):
        self._item_type = item_type

    @classmethod
    def to_json(cls, value: PyType, to_json: Callable) -> JsonType:
        raise RuntimeError('Must be never called')  # pragma: nocover

    def from_json(self, value: JsonType) -> AggregateFunction:
        return AggregateFunction(value)


DEFAULT_CONVERTES = [
    (StrType, ['String', 'Enum8', 'Enum16'], str),
    (StrStripZerosType, ['FixedString']),
    (
        IntType,
        [
            'UInt8', 'UInt16', 'UInt32', 'UInt64',
            'Int8', 'Int16', 'Int32', 'Int64',
        ],
        [int, bool],
    ),
    (FloatType, ['Float32', 'Float64'], float),
    # It looks like Decimal32/Decimal64/Decimal128 were used for some period
    # (reproducable with Clickhouse 20.1.2.4)
    (DecimalType, ['Decimal', 'Decimal32', 'Decimal64', 'Decimal128'], Decimal),
    (DateType, ['Date'], date),
    (DateTimeType, ['DateTime'], datetime),
    (UUIDType, ['UUID'], UUID),
    (IPv4Type, ['IPv4'], IPv4Address),
    (IPv6Type, ['IPv6'], IPv6Address),
    (NothingType, ['Nothing'], NoneType),
    (TupleType, ['Tuple'], tuple),
    (ArrayType, ['Array'], list),
    (NullableType, ['Nullable']),
    (ProxyType, ['LowCardinality', 'SimpleAggregateFunction']),
    (AggregateFunctionType, ['AggregateFunction']),
]


class TypeRegistry:

    def __init__(self, converters=DEFAULT_CONVERTES):
        self._types = {}
        self._escapers = {}
        self._to_json = {}
        for args in converters:
            self.register(*args)

    def register(
        self,
        conv_class: Type[BaseType],
        ch_types: Iterable[str],
        py_types: Union[type, Iterable[type]] = (),
    ):
        for ch_type in ch_types:
            self._types[ch_type] = conv_class
        if isinstance(py_types, type):
            py_types = [py_types]
        for py_type in py_types:
            assert isinstance(py_type, type)
            self._escapers[py_type] = conv_class.escape
            self._to_json[py_type] = conv_class.to_json

    def __getitem__(self, ch_type_name):
        return self._types[ch_type_name]

    def escape(self, value):
        py_type = type(value)
        try:
            return self._escapers[py_type](value, self.escape)
        except KeyError:
            # Fallback to slower method
            for subclass in py_type.mro()[1:]:
                if subclass in self._escapers:
                    escape = self._escapers[subclass]
                    # Cache to speed up further look-ups
                    self._escapers[py_type] = escape
                    return escape(value, self.escape)
            else:
                raise TypeError(f'Unsupported type {py_type}')

    def to_json(self, value):
        py_type = type(value)
        try:
            return self._to_json[py_type](value, self.to_json)
        except KeyError:
            # Fallback to slower method
            for subclass in py_type.mro()[1:]:
                if subclass in self._to_json:
                    to_json = self._to_json[subclass]
                    # Cache to speed up further look-ups
                    self._to_json[py_type] = to_json
                    return to_json(value, self.to_json)
            else:
                raise TypeError(f'Unsupported type {py_type}')
