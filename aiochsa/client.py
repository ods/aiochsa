import re

from aiochclient import ChClient
from clickhouse_sqlalchemy.drivers.http.base import dialect
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
from sqlalchemy.sql import func, ClauseElement
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.ddl import DDLElement


RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
        r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
        r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)

_dialect = dialect()
_escaper = Escaper()


def execute_defaults(query, args):
    if isinstance(query, Insert):
        attr_name = 'default'
    elif isinstance(query, Update):
        attr_name = 'onupdate'
    else:
        return query

    if not args:
        # query.parameters could be a list in a multi row insert
        args = query.parameters
        if not isinstance(query.parameters, list):
            args = [args]

    for params in args:
        _execute_default_attr(query, params, attr_name)
    return query

def _execute_default_attr(query, param, attr_name):
    # XXX Use _process_executesingle_defaults and _process_executemany_defaults
    # method of ExecutionContext? Or get_insert_default and get_update_default?
    # Created from dialect.execution_ctx_cls
    for col in query.table.columns:
        attr = getattr(col, attr_name)
        if attr and param.get(col.name) is None:
            if attr.is_sequence:
                param[col.name] = func.nextval(attr.name)
            elif attr.is_scalar:
                param[col.name] = attr.arg
            elif attr.is_callable:
                param[col.name] = attr.arg({})


def compile_query(query, args):
    if isinstance(query, str):
        return query, args
    elif isinstance(query, DDLElement):
        compiled = query.compile(dialect=_dialect)
        assert not args
        return compiled.string % _escaper.escape(compiled.params), []
    elif isinstance(query, ClauseElement):
        query = execute_defaults(query, args)
        compiled = query.compile(dialect=_dialect)
        # TODO Analyse source query instead of compiled string
        m = RE_INSERT_VALUES.match(compiled.string)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()
            values_list = [
                q_values % _escaper.escape(parameters)
                for parameters in (args or [compiled.params])
            ]
            query = '{} {};'.format(q_prefix, ','.join(values_list))
            return query, []
        else:
            assert not args
            query = compiled.string % _escaper.escape(compiled.params)
            return query, []
    else:
        assert False, type(query)


class ChClientSa(ChClient):

    async def _execute(self, query: str, *args):
        query, args = compile_query(query, args)
        async for rec in super()._execute(query, *args):
            yield rec

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
