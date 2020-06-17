from typing import Optional, Union
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from aiohttp.client import ClientSession, ClientTimeout

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

    params = dict(parse_qsl(parsed.query))

    return {
        **params,
        'url': urlunsplit(('http', netloc, '', '', '')),
        'database': database,
        'user': parsed.username,
        'password': parsed.password,
    }


class Pool:

    DEFAULT_TIMEOUT = {
        'total': 5*60,
        'connect': None,
        'sock_read': None,
        'sock_connect': 10,
    }

    def __init__(
        self, dsn, session_class=ClientSession,
        session_timeout: Optional[Union[float, int, dict]] = None,
        client_class=Client, **params,
    ):
        timeout_params = self.DEFAULT_TIMEOUT.copy()
        if isinstance(session_timeout, dict):
            timeout_params.update(session_timeout)
        else:
            timeout_params['total'] = session_timeout
        self._session = session_class(
            timeout=ClientTimeout(**timeout_params)
        )
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
