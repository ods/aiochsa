from datetime import datetime
from decimal import Decimal
import os

import clickhouse_sqlalchemy
import pytest
import sqlalchemy as sa

import aiochsa
from aiochsa import error_codes
from aiochsa.dialect import ClickhouseSaDialect


def pytest_addoption(parser):
    parser.addoption(
        '--clickhouse-version', default=None,
        help='use specified version of ClickHouse',
    )


@pytest.fixture(scope='session')
def dsn(request, docker_services):
    ch_version = request.config.getoption('--clickhouse-version')
    if ch_version is not None:
        os.environ['CLICKHOUSE_VERSION'] = ch_version
    docker_services.start('clickhouse-server')
    public_port = docker_services.wait_for_service('clickhouse-server', 8123)
    return f'clickhouse://{docker_services.docker_ip}:{public_port}'


_clickhouse_version = None


@pytest.fixture
async def clickhouse_version(dsn):
    global _clickhouse_version
    if _clickhouse_version is None:
        async with aiochsa.connect(dsn) as conn:
            version = await conn.fetchval('SELECT version()')
            _clickhouse_version = tuple(
                int(num) for num in version.split('.')
            )
    return _clickhouse_version


@pytest.fixture
async def pool(dsn):
    async with aiochsa.create_pool(dsn) as pool:
        yield pool


@pytest.fixture
async def conn(dsn):
    async with aiochsa.connect(dsn) as conn:
        yield conn


@pytest.fixture
def recreate_table(conn):

    async def _recreate(table_name, create_ddl):
        try:
            await conn.execute(f'DROP TABLE {table_name}')
        except aiochsa.DBException as exc:
            if exc.code != error_codes.UNKNOWN_TABLE:
                raise

        await conn.execute(create_ddl)

    return _recreate


TEST1_CREATE_DDL = '''\
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


@pytest.fixture
async def table_test(recreate_table):
    await recreate_table('test', TEST1_CREATE_DDL)

    return sa.Table(
        'test', sa.MetaData(),
        sa.Column('id', sa.Integer),
        sa.Column('enum', sa.Enum, default=''),
        sa.Column('name', sa.String),
        sa.Column('timestamp', sa.DateTime, default=datetime.utcnow),
        sa.Column('amount', sa.DECIMAL, default=Decimal(0)),
    )


TEST_MT_CREATE_DDL = '''\
CREATE TABLE test_mt
(
    title String,
    num UInt64
)
ENGINE = MergeTree()
ORDER BY num
'''


@pytest.fixture
async def table_mt(recreate_table):
    await recreate_table('test_mt', TEST_MT_CREATE_DDL)

    return sa.Table(
        'test_mt', sa.MetaData(),
        sa.Column('num', sa.Integer),
        sa.Column('title', sa.String),
    )


TEST_SMT_CREATE_DDL = '''\
CREATE TABLE table_smt
(
    key UInt8,
    value UInt64
)
ENGINE = SummingMergeTree()
ORDER BY key
'''


@pytest.fixture
async def table_smt(recreate_table):
    await recreate_table('table_smt', TEST_SMT_CREATE_DDL)

    return sa.Table(
        'table_smt', sa.MetaData(),
        sa.Column('key', sa.Integer),
        sa.Column('value', sa.Integer),
    )


TEST_AMT_CREATE_DDL = '''\
CREATE TABLE test_amt
(
    key UInt8,
    value AggregateFunction(sum, UInt8)
)
ENGINE = AggregatingMergeTree()
ORDER BY key
'''


@pytest.fixture
async def table_amt(recreate_table):
    await recreate_table('test_amt', TEST_AMT_CREATE_DDL)

    return sa.Table(
        'test_amt', sa.MetaData(),
        sa.Column('key', sa.Integer),
        sa.Column('value', sa.Integer),
    )


TABLE_FOR_TYPE_DDL_TEMPLATE = '''\
CREATE TABLE test_for_type
(
    value {type}
)
ENGINE = Log()
'''


@pytest.fixture
def recreate_table_for_type(recreate_table):

    async def _recreate(type_str):
        create_ddl = TABLE_FOR_TYPE_DDL_TEMPLATE.format(type=type_str)
        await recreate_table('test_for_type', create_ddl)
        return 'test_for_type'

    return _recreate


@pytest.fixture
def table_for_type(recreate_table_for_type):

    async def _create(sa_type):
        if isinstance(sa_type, type):
            sa_type = sa_type()
        type_str = sa_type.compile(dialect=ClickhouseSaDialect())

        table_name = await recreate_table_for_type(type_str)

        return sa.Table(
            table_name, sa.MetaData(),
            sa.Column('value', sa_type),
        )

    return _create


@pytest.fixture(params=[
    sa.select,
    clickhouse_sqlalchemy.select,
    aiochsa.select,
])
def any_select(request):
    return request.param
