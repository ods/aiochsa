import asyncio
import pkgutil
import simplejson as json
from typing import AsyncGenerator, Iterable, Union

import aiohttp
from lark import Lark, Transformer, v_args

from .record import Record
from .types import TypeRegistry


__all__ = ['parse_type']


type_parser = Lark(
    pkgutil.get_data(__name__, 'type.lark').decode(),
    parser='lalr',
)


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
        return self._types[name]()


def parse_type(types: TypeRegistry, type_str):
    tree = type_parser.parse(type_str)
    return TypeTransformer(types).transform(tree)


async def parse_json_compact(
    types: TypeRegistry,
    content: Union[asyncio.StreamReader, aiohttp.StreamReader],
) -> Iterable[Record]:
    # The method is split into three phases:
    #   1. Read whole response.  It's done immediately after await-ing, so we
    #      can close response after it.
    #   2. Parse type information from meta.  It's done at first iteration of
    #      returned async-generator.
    #   3. Convert each row one-by-one.  It's done on demand at each iteration.
    # This way we can cleanup resources even when result is not used.

    json_data = json.loads(await content.read(), parse_float=str)
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
