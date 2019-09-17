from typing import AsyncIterable

from aiochclient.client import ChClient, ChClientError, QueryTypes

from .compiler import compile_statement
from .exc import DBException
from .record import Record, RecordFabric


class ChClientSa(ChClient):

    async def _execute(self, statement: str, *args) -> AsyncIterable[Record]:
        query = compile_statement(statement, args)

        # The rest is a modified copy of `ChClient._execute()`
        query_type = self.query_type(query)

        if query_type == QueryTypes.FETCH:
            query += ' FORMAT TSVWithNamesAndTypes'
        data = query.encode()

        async with self._session.post(
            self.url, params=self.params, data=data
        ) as resp:
            if resp.status != 200:
                body = await resp.read()
                raise DBException.from_message(body.decode(errors='replace'))

            if query_type == QueryTypes.FETCH:
                record_fabric = RecordFabric(
                    names_line=await resp.content.readline(),
                    types_line=await resp.content.readline(),
                )
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
