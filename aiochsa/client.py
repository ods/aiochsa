from typing import AsyncIterable

from aiochclient.client import ChClient

from .compiler import Compiler
from .exc import DBException
from .record import Record, RecordFabric


class ChClientSa(ChClient):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._compiler = Compiler()

    async def _execute(self, statement: str, *args) -> AsyncIterable[Record]:
        query = self._compiler.compile_statement(statement, args)

        # The rest is a modified copy of `ChClient._execute()`
        data = query.encode()

        async with self._session.post(
            self.url,
            params={'default_format': 'TSVWithNamesAndTypes', **self.params},
            data=data,
        ) as resp:
            if resp.status != 200:
                body = await resp.read()
                raise DBException.from_message(body.decode(errors='replace'))

            if resp.content_type == 'text/tab-separated-values':
                names_line = await resp.content.readline()
                if not names_line:  # It's INSERT
                    return
                types_line = await resp.content.readline()
                record_fabric = RecordFabric(names_line, types_line)
                async for line in resp.content:
                    yield record_fabric.parse_row(line)

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
