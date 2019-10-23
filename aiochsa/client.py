from typing import AsyncGenerator

from aiochclient.client import ChClient
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http

from .compiler import Compiler
from .exc import DBException
from .parser import parse_json_compact
from .record import Record
from .types import TypeRegistry


class ChClientSa(ChClient):

    def __init__(self, *args, dialect=None, types=None, **kwargs):
        super().__init__(*args, **kwargs)
        if dialect is None: # pragma: no cover
            # XXX Do we actualy need the ability to pass custom dialect?
            dialect = ClickHouseDialect_http()
        if types is None:
            types = TypeRegistry()
        self._types = types
        self._compiler = Compiler(dialect=dialect, encode=types.encode)

    async def _execute(
        self, statement: str, *args,
    ) -> AsyncGenerator[Record, None]:
        query = self._compiler.compile_statement(statement, args)

        # The rest is a modified copy of `ChClient._execute()`
        data = query.encode()

        async with self._session.post(
            self.url,
            params = {'default_format': 'JSONCompact', **self.params},
            data = data,
        ) as resp:
            if resp.status != 200:
                body = await resp.read()
                raise DBException.from_message(body.decode(errors='replace'))

            if resp.content_type == 'application/json':
                async for row in parse_json_compact(self._types, resp.content):
                    yield row

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
