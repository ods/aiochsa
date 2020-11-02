from collections import namedtuple
import pkgutil
import simplejson as json
from typing import Iterable

from lark import Lark, Transformer, v_args

from .record import Record
from .types import TypeRegistry


__all__ = ['parse_type', 'parse_json_compact', 'JSONDecodeError']


# Re-export
JSONDecodeError = json.JSONDecodeError


type_parser = Lark(
    pkgutil.get_data(__name__, 'type.lark').decode(),
    parser='lalr',
)


EnumOption = namedtuple('EnumOption', ['label', 'value'])


@v_args(inline=True)
class TypeTransformer(Transformer):

    def __init__(self, types: TypeRegistry):
        self._types = types

    def start(self, type_):
        return type_

    def composite_type(self, name, *types):
        return self._types[name](*types)

    def aggregate_type(self, name, func, type_):
        return self._types[name](type_)

    def simple_type(self, name, *params):
        return self._types[name](*params)

    def enum_param(self, label, value):
        return EnumOption(label, value)

    def STRING(self, value):
        assert value[0] == value[-1] == "'"
        return value[1:-1]

    def INT(self, value):
        return int(value)


def parse_type(types: TypeRegistry, type_str):
    tree = type_parser.parse(type_str)
    return TypeTransformer(types).transform(tree)


def parse_json_compact(
    types: TypeRegistry, content: bytes,
) -> Iterable[Record]:
    # The method is split into three phases:
    #   1. Parse JSON.  It's done immediately, so that we can fall back to
    #      parsing exception when it breaks normal response.
    #   2. Parse type information from meta.  It's done at first iteration of
    #      returned generator.
    #   3. Convert each row one-by-one.  It's done on demand at each iteration.
    # This way we can cleanup resources even when result is not used.

    json_data = json.loads(content, parse_float=str)
    return convert_json_compact(types, json_data)


def convert_json_compact(
    types: TypeRegistry, json_data: dict,
) -> Iterable[Record]:
    names = []
    converters = []
    for column in json_data['meta']:
        names.append(column['name'])
        type_obj = parse_type(types, column['type'])
        converters.append(type_obj.from_json)

    for row in json_data['data']:
        yield Record(
            names = names,
            values = [
                converter(value)
                for converter, value in zip(converters, row)
            ]
        )
