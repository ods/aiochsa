from aiochclient import ChClient

from .compiler import compile_statement


class ChClientSa(ChClient):

    async def _execute(self, statement: str, *args):
        statement = compile_statement(statement, args)
        async for rec in super()._execute(statement):
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
