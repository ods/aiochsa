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


if __name__ == '__main__':
    # TODO Covert this to proper tests
    tree = type_parser.parse(
        r"Tuple("
            r"UInt8, "
            r"Tuple(UInt8, Nullable(Nothing), String, Array(String)), "
            r"Decimal(9, 2), "
            r"Date, "
            r"IPv4, IPv6,"
            r"Nullable(Enum8('' = 0, 'a' = 1, '\t\n\0\\\'' = 2))"
        r")",
    )

    print(tree.pretty())

    t = TypeTransformer(TypeRegistry()).transform(tree)
