from typing import AsyncIterable

from aiochclient.client import ChClient, ChClientError, QueryTypes
from aiochclient.records import Record, RecordsFabric # TODO To be replaced

from .compiler import compile_statement
from .exc import DBException


class ChClientSa(ChClient):

    async def _execute(self, statement: str, *args) -> AsyncIterable[Record]:
        query = compile_statement(statement, args)

        # The rest is a modified copy of `ChClient._execute()`
        query_type = self.query_type(query)

        if query_type == QueryTypes.FETCH:
            query += " FORMAT TSVWithNamesAndTypes"
        data = query.encode()

        async with self._session.post(
            self.url, params=self.params, data=data
        ) as resp:  # type: client.ClientResponse
            if resp.status != 200:
                # TODO Parse code and display test (and stack trace?):
                # https://github.com/yandex/ClickHouse/blob/master/dbms/src/Common/Exception.cpp#L261
                raise DBException.from_response(
                    (await resp.read()).decode(errors='replace')
                )
            if query_type == QueryTypes.FETCH:
                rf = RecordsFabric(
                    names=await resp.content.readline(),
                    tps=await resp.content.readline(),
                )
                async for line in resp.content:
                    yield rf.new(line)

    def __await__(self):
        # For compartibility with asyncpg (`await pool.acquire(...)`)
        yield from []
        return self

    # Allow using client as context manager when returned from `Pool.acquire()`
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
