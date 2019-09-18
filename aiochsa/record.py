from collections.abc import Mapping
from aiochclient.types import what_py_converter


class Record(Mapping):

    __slots__ = ('_names', '_values')

    def __init__(self, names, values):
        self._names = names
        self._values = values

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def __getitem__(self, item):
        if isinstance(item, str):
            try:
                item = self._names.index(item)
            except ValueError:
                raise KeyError(item)
        return self._values[item]

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, IndexError):
            return default

    def keys(self):
        return self._names

    def values(self):
        return self._values

    def items(self):
        return zip(self._names, self._values)

    def __eq__(self, other):
        if isinstance(other, Mapping):
            return super().__eq__(other)
        else:
            return self._values == list(other)

    def __repr__(self):
        return '<Record {}>'.format(
            ' '.join(
                f'{name}={value!r}'
                for name, value in self.items()
            )
        )


class RecordFabric:

    def __init__(self, names_line, types_line):
        self._names = names_line.decode().rstrip().split('\t')
        self._converters = [
            what_py_converter(type_name)
            for type_name in types_line.decode().rstrip().split('\t')
        ]

    def parse_row(self, row_line):
        row_line = row_line.rstrip(b'\n')
        return Record(
            names=self._names,
            values=[
                converter(value)
                for converter, value in zip(self._converters, row_line.split(b'\t'))
            ]
        )
