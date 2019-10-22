from datetime import date, datetime, timezone
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import (
    Any, Callable, Generic, Iterable, List, Optional, Type, TypeVar, Union,
)
from uuid import UUID


NoneType = type(None)
PyType = TypeVar('PyType')
JsonType = TypeVar('JsonType', NoneType, int, str, list)


class BaseType(Generic[PyType, JsonType]):
    __slots__ = ()

    py_type: Optional[Type[PyType]] = None

    @classmethod
    def encode(cls, value: PyType, encode: Callable) -> str:
        return str(value)

    def from_json(self, value: JsonType) -> PyType:
        return self.py_type(value)

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

    @classmethod
    def encode(cls, value: str, encode=None) -> str:
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

    @classmethod
    def encode(cls, value: PyType, encode: Callable) -> str:
        return f"'{value}'"


class DateType(BaseType):
    py_type = date

    @classmethod
    def encode(cls, value: date, encode=None) -> str:
        return f"'{value.isoformat()}'"

    def from_json(self, value: str) -> Optional[date]:
        if value == '0000-00-00':
            return None
        return date.fromisoformat(value)


class DateTimeType(BaseType):
    py_type = datetime

    @classmethod
    def encode(cls, value: datetime, encode=None) -> str:
        value = value.replace(tzinfo=None, microsecond=0)
        return f"'{value.isoformat()}'"

    def from_json(self, value: str) -> Optional[datetime]:
        if value == '0000-00-00 00:00:00':
            return None
        return datetime.fromisoformat(value)


class DateTimeUTCType(DateTimeType):

    @classmethod
    def encode(cls, value: datetime, encode=None) -> str:
        if value.utcoffset() is None:
            raise ValueError(
                'Got naive datetime while timezone-aware is expected'
            )
        value = (
            value.astimezone(timezone.utc)
                .replace(tzinfo=None, microsecond=0)
        )
        return f"'{value.isoformat()}'"

    def from_json(self, value: str) -> datetime:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


class UUIDType(BaseType):
    py_type = UUID

    @classmethod
    def encode(cls, value: UUID, encode=None) -> str:
        return f"'{value}'"


class IPv4Type(BaseType):
    py_type = IPv4Address


class IPv6Type(BaseType):
    py_type = IPv6Address


class NothingType(BaseType):
    py_type = NoneType

    def encode(self, value: NoneType, encode=None) -> str:
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
    def encode(cls, value: tuple, encode=None) -> str:
        return '({})'.format(
            ','.join(encode(v) for v in value)
        )

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
    def encode(cls, value: list, encode) -> str:
        return '[{}]'.format(
            ','.join(encode(v) for v in value)
        )

    def from_json(self, value: List[JsonType]) -> list:
        return [self._item_type.from_json(v) for v in value]


class NullableType(BaseType):
    __slots__ = ('_item_type',)

    def __init__(self, item_type):
        self._item_type = item_type

    @classmethod
    def encode(cls, value: PyType, encode=None) -> str:
        raise RuntimeError('Must be never called')  # pragma: nocover

    def from_json(self, value: JsonType) -> Any:
        if value is None:
            return
        return self._item_type.from_json(value)


class LowCardinalityType(BaseType):
    __slots__ = ('_item_type',)

    def __init__(self, item_type):
        self._item_type = item_type

    @classmethod
    def encode(cls, value: PyType, encode=None) -> str:
        raise RuntimeError('Must be never called')  # pragma: nocover

    def from_json(self, value: JsonType) -> Any:
        return self._item_type.from_json(value)


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
    (DecimalType, ['Decimal'], Decimal),
    (DateType, ['Date'], date),
    (DateTimeType, ['DateTime'], datetime),
    (UUIDType, ['UUID'], UUID),
    (IPv4Type, ['IPv4'], IPv4Address),
    (IPv6Type, ['IPv6'], IPv6Address),
    (NothingType, ['Nothing'], NoneType),
    (TupleType, ['Tuple'], tuple),
    (ArrayType, ['Array'], list),
    (NullableType, ['Nullable']),
    (LowCardinalityType, ['LowCardinality']),
]


class TypeRegistry:

    def __init__(self, converters=DEFAULT_CONVERTES):
        self._types = {}
        self._encoders = {}
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
            self._encoders[py_type] = conv_class.encode

    def __getitem__(self, ch_type_name):
        return self._types[ch_type_name]

    def encode(self, value):
        py_type = type(value)
        try:
            return self._encoders[py_type](value, self.encode)
        except KeyError:
            # Fallback to slower method
            for subclass in py_type.mro()[1:]:
                if subclass in self._encoders:
                    encode = self._encoders[subclass]
                    # Cache to speed up further look-ups
                    self._encoders[py_type] = encode
                    return encode(value, self.encode)
            else:
                raise TypeError(f'Unsupported type {py_type}')
