import asyncio
from urllib.parse import urlsplit, urlunsplit

from aiohttp.client import ClientSession

from .client import ChClientSa


def dsn_to_connect_url(dsn):
    parsed = urlsplit(dsn)

    if parsed.scheme != 'clickhouse':
        raise ValueError(
            f'Expecting "clickhouse" scheme in DSN, got {parsed.scheme}'
        )

    # Set default port. Keep username/password unchanged if any.
    netloc = parsed.netloc
    if parsed.port is None:
        netloc += ':8123'

    # XXX Parse parameters from query?

    return urlunsplit(('http', netloc, parsed.path, parsed.query, ''))


class Pool:

    def __init__(self, dsn):
        # TODO Session and connector parameters
        self._session = ClientSession()
        url = dsn_to_connect_url(dsn)
        self._client = ChClientSa(self._session, url=url)

    async def close(self):
        await self._session.close()

    def __await__(self):
        # For compartibility with asyncpg
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def acquire(self):
        return self._client

    async def release(self, conn):
        pass

    async def execute(self, *args, **kwargs):
        return await self._client.execute(*args, **kwargs)

    async def fetch(self, *args, **kwargs):
        return await self._client.fetch(*args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        return await self._client.fetchrow(*args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        return await self._client.fetchval(*args, **kwargs)


def connect(dsn):
    return Pool(dsn)

def create_pool(dsn):
    return Pool(dsn)
