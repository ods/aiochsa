import logging
import simplejson as json
from typing import Any, AsyncGenerator, Iterable, List, Optional

import aiohttp

from .compiler import Compiler, Statement
from .dialect import ClickhouseSaDialect
from .exc import DBException, ProtocolError, exc_message_re
from .parser import parse_json_compact, JSONDecodeError
from .record import Record
from .types import TypeRegistry


logger = logging.getLogger(__name__)
sql_logger = logging.getLogger(f'{__name__}.SQL')


class Client:

    def __init__(
        self, session: aiohttp.ClientSession, *, url='http://localhost:8123/',
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

    async def _execute(self, statement: Statement, *args) -> Iterable[Record]:
        compiled, json_each_row_parameters = self._compiler.compile_statement(
            statement, args,
        )
        sql_logger.debug(compiled)
        compiled_with_params = compiled
        rows = None
        if json_each_row_parameters:
            to_json = self._types.to_json # lookup optimization
            rows = [
                json.dumps(
                    {name: to_json(value) for name, value in row.items()},
                    use_decimal=True,
                )
                for row in json_each_row_parameters
            ]
            if sql_logger.isEnabledFor(logging.DEBUG):
                for idx, row in enumerate(rows):
                    sql_logger.debug(f'{idx}: {row}')
            compiled_with_params += '\n' + '\n'.join(rows)

        # First attempt may fail due to broken state of aiohttp session
        # (aiohttp doesn't handle connection closing properly?)
        for retrying in [False, True]:
            try:
                async with self._session.post(
                    self.url,
                    params = {'default_format': 'JSONCompact', **self.params},
                    data = compiled_with_params.encode(),
                ) as response:
                    body = await response.read()
                    if response.status != 200:
                        raise DBException.from_message(
                            body.decode(errors='replace'),
                            statement=compiled, rows=rows,
                        )

                    elif response.content_type == 'application/json':
                        try:
                            return parse_json_compact(self._types, body)
                        except JSONDecodeError:
                            body_str = body.decode(errors='replace')
                            m = exc_message_re.search(body_str)
                            if not m:
                                raise
                            raise DBException.from_message(
                                body_str[m.start():],
                                statement=compiled, rows=rows,
                            )
                    else:
                        return ()
            except aiohttp.ClientError as exc:
                if retrying:
                    raise ProtocolError(exc) from exc
                logger.debug(f'First attempt failed, retrying (error: {exc})')

    async def iterate(
        self, statement: Statement, *args,
    ) -> AsyncGenerator[Record, None]:
        for row in await self._execute(statement, *args):
            yield row

    async def execute(self, statement: Statement, *args) -> None:
        await self._execute(statement, *args)

    async def fetch(self, statement: Statement, *args) -> List[Record]:
        return list(await self._execute(statement, *args))

    async def fetchrow(self, statement: Statement, *args) -> Optional[Record]:
        gen = await self._execute(statement, *args)
        return next(iter(gen), None)

    async def fetchval(self, statement: Statement, *args) -> Any:
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
