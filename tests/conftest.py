import asyncio
from datetime import datetime
from decimal import Decimal

from aiochclient import ChClientError
import pytest
import sqlalchemy as sa

import aiochsa


def pytest_collection_modifyitems(items):
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker('asyncio')


@pytest.fixture(scope='session')
def dsn(docker_services):
    docker_services.start('clickhouse-server')
    public_port = docker_services.wait_for_service('clickhouse-server', 8123)
    return f'clickhouse://{docker_services.docker_ip}:{public_port}'


@pytest.fixture
async def conn(dsn):
    async with aiochsa.connect(dsn) as conn:
        yield conn


TEST_CREATE_DDL = '''\
CREATE TABLE test
(
    id UInt64,
    enum Enum8(''=0, 'ONE'=1, 'TWO'=2),
    name String,
    timestamp DateTime,
    amount Decimal64(9)
)
ENGINE = MergeTree()
ORDER BY id
'''

TEST_DROP_DDL = 'DROP TABLE test'


@pytest.fixture
async def test_table(conn):
    try:
        await conn.execute(TEST_DROP_DDL)
    except ChClientError:
        pass
    await conn.execute(TEST_CREATE_DDL)
    return sa.Table(
        'test', sa.MetaData(),
        sa.Column('id', sa.Integer),
        sa.Column('enum', sa.Enum, default=''),
        sa.Column('name', sa.String),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow),
        sa.Column('amount', sa.DECIMAL, default=Decimal(0)),
    )
