import asyncio
from urllib.parse import urlsplit, urlunsplit

from aiohttp.client import ClientSession

from .client import Client


def dsn_to_params(dsn):
    parsed = urlsplit(dsn)

    if parsed.scheme != 'clickhouse':
        raise ValueError(
            f'Expecting "clickhouse" scheme in DSN, got {parsed.scheme}'
        )

    hostname = parsed.hostname or '127.0.0.1'
    port = parsed.port or 8123
    netloc = f'{hostname}:{port}'

    database = parsed.path.lstrip('/')
    if not database:
        database = 'default'

    # XXX Parse parameters from query?

    return {
        'url': urlunsplit(('http', netloc, '', '', '')),
        'database': database,
        'user': parsed.username,
        'password': parsed.password,
    }


class Pool:

    def __init__(self, dsn, client_class=Client, **params):
        # TODO Session and connector parameters
        self._session = ClientSession()
        params.update(dsn_to_params(dsn))
        self._client = client_class(self._session, **params)

    async def close(self):
        await self._session.close()

    def __await__(self):
        # For compartibility with asyncpg (`await create_pool(...)`)
        yield from []
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def acquire(self, *, timeout=None):
        return self._client

    async def release(self, conn, *, timeout=None):
        pass

    async def iterate(self, *args, **kwargs):
        async for row in self._client.iterate(*args, **kwargs):
            yield row

    async def execute(self, *args, **kwargs):
        return await self._client.execute(*args, **kwargs)

    async def fetch(self, *args, **kwargs):
        return await self._client.fetch(*args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        return await self._client.fetchrow(*args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        return await self._client.fetchval(*args, **kwargs)


def connect(dsn, **kwargs):
    return Pool(dsn, **kwargs)

def create_pool(dsn, **kwargs):
    return Pool(dsn, **kwargs)
