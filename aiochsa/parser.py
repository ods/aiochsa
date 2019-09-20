from pathlib import Path
from lark import Lark, Transformer, v_args

from .types import TypeRegistry

__all__ = ['parse_type']


CUR_DIR = Path(__file__).parent
type_parser = Lark(open(CUR_DIR / 'type.lark').read())


@v_args(inline=True)
class TypeTransformer(Transformer):

    def __init__(self, types: TypeRegistry):
        self._types = types

    def composite_type(self, name, *types):
        return self._types[name](types)

    def simple_type(self, name, *params):
        return self._types[name]()


def parse_type(types: TypeRegistry, type_str):
    tree = type_parser.parse(type_str)
    return TypeTransformer(types).transform(tree)
