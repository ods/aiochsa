from types import SimpleNamespace

from aiochclient import ChClient
from clickhouse_sqlalchemy.drivers.http.base import dialect
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
from sqlalchemy.sql import func, ClauseElement
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import Insert


_dialect = dialect()
_escaper = Escaper()


def _execute_clauseelement(elem, multiparams):
    if multiparams:
        if len(multiparams) == 1 and isinstance(multiparams[0], dict):
            multiparams = multiparams[0]
        if isinstance(elem, Insert):
            elem = elem.values(multiparams)
        else:
            elem = elem.params(multiparams)
    compiled_sql = elem.compile(
        dialect=_dialect,
    )
    return _execute_context(
        _dialect,
        _dialect.execution_ctx_cls._init_compiled,
        compiled_sql,
        (),
        compiled_sql,
        (),
    )

def _execute_ddl(ddl, multiparams):
    compiled = ddl.compile(dialect=_dialect)
    return _execute_context(
        _dialect,
        _dialect.execution_ctx_cls._init_ddl,
        compiled,
        None,
        compiled,
    )


def _execute_context(dialect, constructor, statement, parameters, *args):
    conn = SimpleNamespace(dialect=dialect, _execution_options={})
    db_api_conn = SimpleNamespace(cursor=lambda: None)
    context = constructor(dialect, conn, db_api_conn, *args)
    assert len(context.parameters) == 1
    return context.statement % _escaper.escape(context.parameters[0])


def compile_query(query, args):
    if isinstance(query, str):
        assert not args
        return query
    elif isinstance(query, ClauseElement):
        if isinstance(query, DDLElement):
            query = _execute_ddl(query, args)
        else:
            query = _execute_clauseelement(query, args)
        return query
    else: # pragma: no cover
        assert False, type(query)


class ChClientSa(ChClient):

    async def _execute(self, query: str, *args):
        query = compile_query(query, args)
        async for rec in super()._execute(query):
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
