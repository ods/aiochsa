import re

from aiochclient import ChClient
from clickhouse_sqlalchemy.drivers.http.base import dialect
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
from sqlalchemy.sql import func, ClauseElement
from sqlalchemy.sql.dml import Insert


RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
        r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
        r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)

_dialect = dialect()
_escaper = Escaper()


def execute_defaults(query, args):
    # Clickhouse doesn't support updates, so we don't need to honor `onupdate`
    if not isinstance(query, Insert):
        return query

    if not args:
        # query.parameters could be a list in a multi row insert
        args = query.parameters
        if not isinstance(query.parameters, list):
            args = [args]

    for params in args:
        _execute_default_attr(query, params)
    return query

def _execute_default_attr(query, params):
    # XXX Use _process_executesingle_defaults and _process_executemany_defaults
    # method of ExecutionContext? Or get_insert_default?
    # Created from dialect.execution_ctx_cls
    for col in query.table.columns:
        attr = col.default
        if attr and col.name not in params:
            # Clickhouse doesn't support sequences, so we skip them
            if attr.is_scalar:
                params[col.name] = attr.arg
            elif attr.is_callable:
                params[col.name] = attr.arg({})
            else: # pragma: no cover
                assert False, repr(attr)


def compile_query(query, args):
    if isinstance(query, str):
        return query, args
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
            if compiled.params is None:
                query = compiled.string
            else:
                query = compiled.string % _escaper.escape(compiled.params)
            return query, []
    else: # pragma: no cover
        assert False, type(query)


class ChClientSa(ChClient):

    async def _execute(self, query: str, *args):
        query, args = compile_query(query, args)
        async for rec in super()._execute(query, *args):
            yield rec

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
