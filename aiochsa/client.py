import simplejson as json
from typing import Any, AsyncGenerator, Iterable, List, Optional

from aiohttp import ClientSession

from .compiler import Compiler
from .dialect import ClickhouseSaDialect
from .exc import DBException
from .parser import parse_json_compact
from .record import Record
from .types import TypeRegistry


class Client:

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
            dialect = ClickhouseSaDialect()
        if types is None:
            types = TypeRegistry()
        self._types = types
        self._compiler = Compiler(dialect=dialect, escape=types.escape)

    async def _execute(self, statement: str, *args) -> Iterable[Record]:
        query, json_each_row_parameters = self._compiler.compile_statement(
            statement, args,
        )
        if json_each_row_parameters:
            to_json = self._types.to_json # lookup optimization
            query += '\n'
            query += '\n'.join(
                json.dumps(
                    {name: to_json(value) for name, value in row.items()},
                    use_decimal=True,
                )
                for row in json_each_row_parameters
            )

        async with self._session.post(
            self.url,
            params = {'default_format': 'JSONCompact', **self.params},
            data = query.encode(),
        ) as response:
            if response.status != 200:
                body = await response.read()
                raise DBException.from_message(
                    query, body.decode(errors='replace'),
                )

            if response.content_type == 'application/json':
                return await parse_json_compact(self._types, response.content)
            else:
                return ()

    async def iterate(
        self, statement: str, *args,
    ) -> AsyncGenerator[Record, None]:
        for row in await self._execute(statement, *args):
            yield row

    async def execute(self, statement: str, *args) -> None:
        await self._execute(statement, *args)

    async def fetch(self, statement: str, *args) -> List[Record]:
        return list(await self._execute(statement, *args))

    async def fetchrow(self, statement: str, *args) -> Optional[Record]:
        gen = await self._execute(statement, *args)
        return next(iter(gen), None)

    async def fetchval(self, statement: str, *args) -> Any:
        row = await self.fetchrow(statement, *args)
        if row is not None:
            return row[0]

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    async def close(self):
        # For compartibility with asyncpg
        pass

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
