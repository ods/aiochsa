import pytest

from aiochsa.pool import dsn_to_params, create_pool



def test_dsn_all_explicit():
    params = dsn_to_params('clickhouse://bob:secret@pet:12345/warehouse')
    assert params == {
        'url': 'http://pet:12345',
        'database': 'warehouse',
        'user': 'bob',
        'password': 'secret',
    }


def test_dsn_wrong_scheme():
    with pytest.raises(ValueError):
        dsn_to_params('postgresql://db')


def test_dsn_default_port():
    params = dsn_to_params('clickhouse://pet')
    assert params['url'] == 'http://pet:8123'


async def test_create_pool_close(dsn):
    pool = await create_pool(dsn)
    await pool.execute('SELECT 1')
    await pool.close()


async def test_pool_async_with(pool):
    async with pool.acquire() as conn:
        await conn.execute('SELECT 1')


async def test_pool_acquire_release(pool):
    conn = await pool.acquire()
    await conn.execute('SELECT 1')
    await pool.release(conn)
