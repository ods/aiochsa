from typing import Any, AsyncGenerator, List, Optional

from aiohttp import ClientSession
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http

from .compiler import Compiler
from .exc import DBException
from .parser import parse_json_compact
from .record import Record
from .types import TypeRegistry


class ChClientSa:

    def __init__(
        self, session: ClientSession, *, url='http://localhost:8123/',
        user=None, password=None, database='default', compress_response=False,
        dialect=None, types=None, **settings,
    ):
        self._session = session
        self.url = url
        self.params = {}
        if user:
            self.params["user"] = user
        if password:
            self.params["password"] = password
        if database:
            self.params["database"] = database
        if compress_response:
            self.params["enable_http_compression"] = 1
        self.params.update(settings)
        if dialect is None: # pragma: no cover
            # XXX Do we actualy need the ability to pass custom dialect?
            dialect = ClickHouseDialect_http()
        if types is None:
            types = TypeRegistry()
        self._types = types
        self._compiler = Compiler(dialect=dialect, encode=types.encode)

    async def iterate(
        self, statement: str, *args,
    ) -> AsyncGenerator[Record, None]:
        query = self._compiler.compile_statement(statement, args)

        async with self._session.post(
            self.url,
            params = {'default_format': 'JSONCompact', **self.params},
            data = query.encode(),
        ) as resp:
            if resp.status != 200:
                body = await resp.read()
                raise DBException.from_message(body.decode(errors='replace'))

            if resp.content_type == 'application/json':
                async for row in parse_json_compact(self._types, resp.content):
                    yield row

    async def execute(self, statement: str, *args) -> None:
        agen = self.iterate(statement, *args)
        async for _ in agen:
            break
        # Needed to finalize context managers and execute finally clauses
        await agen.aclose()

    async def fetch(self, statement: str, *args) -> List[Record]:
        return [row async for row in self.iterate(statement, *args)]

    async def fetchrow(self, statement: str, *args) -> Optional[Record]:
        agen = self.iterate(statement, *args)
        row = None
        async for row in agen:
            break
        # Needed to finalize context managers and execute finally clauses
        await agen.aclose()
        return row

    async def fetchval(self, statement: str, *args) -> Any:
        row = await self.fetchrow(statement, *args)
        if row is not None:
            return row[0]

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
