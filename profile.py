# Usage (in virtualenv):
#   pip install -e .
#   pip install docker py-spy
#   py-spy record -o profile.svg -- profile.py

import asyncio
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import docker
import os
from random import randrange
from time import sleep

import docker
import sqlalchemy as sa

import aiochsa


CREATE_TABLE_DDL = '''\
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

table = sa.Table(
    'test', sa.MetaData(),
    sa.Column('id', sa.Integer),
    sa.Column('enum', sa.Enum, default=''),
    sa.Column('name', sa.String),
    sa.Column('timestamp', sa.DateTime, default=datetime.utcnow),
    sa.Column('amount', sa.DECIMAL, default=Decimal(0)),
)

async def create_table(conn):
    await conn.execute(CREATE_TABLE_DDL)


def get_series(count):
    return [
        {
            'id': idx + 1,
            'enum': 'TWO' if idx else 'ONE',
            'name': os.urandom(10).hex(),
            'timestamp': datetime.utcnow(),
            'amount': Decimal(randrange(1000000)) / 100,
        }
        for idx in range(count)
    ]


async def insert(conn, series):
    await conn.execute(table.insert().values(series))


async def main(address='localhost:8123'):
    async with aiochsa.connect(f'clickhouse://{address}') as conn:
        await create_table(conn)
        series = get_series(10000)
        for _ in range(10):
            await insert(conn, series)


@contextmanager
def clickhouse():
    client = docker.from_env()
    container = client.containers.run(
        'yandex/clickhouse-server', detach=True, publish_all_ports=True,
    )
    port = client.api.port(container.id, 8123)[0]['HostPort']
    sleep(1)
    yield f'127.0.0.1:{port}'
    container.stop()


if __name__ == '__main__':
    with clickhouse() as address:
        asyncio.run(main(address))
