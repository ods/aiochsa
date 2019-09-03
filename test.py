#!/usr/bin/env python

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

from aiochclient import ChClientError
import sqlalchemy as sa

from aiochsa import connect


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


test_table = sa.Table(
    'test', sa.MetaData(),
    sa.Column('id', sa.Integer),
    sa.Column('enum', sa.Enum),
    sa.Column('name', sa.String),
    sa.Column('timestamp', sa.DateTime, default=datetime.utcnow),
    sa.Column('amount', sa.DECIMAL, default=Decimal(0)),
)


async def main():
    async with connect('clickhouse://ch') as client:
        try:
            await client.execute(TEST_DROP_DDL)
        except ChClientError:
            pass
        await client.execute(TEST_CREATE_DDL)

        query = (
            test_table.insert()
                .values(
                    id=1,
                    enum='ONE',
                    name='test',
                    timestamp=datetime.utcnow(),
                    amount=Decimal('1.23'),
                )
        )
        await client.execute(query)

        test_alias = test_table.alias()
        query = (
            sa.select(test_table.c)
                .select_from(
                    test_table.join(
                        test_alias,
                        test_table.c.id == test_alias.c.id,
                    )
                )
        )
        print(query)
        rows = await client.fetch(query)
        for row in rows:
            print(dict(row))

        await client.execute(
            test_table.insert(),
            *[
                {
                    'id': 10 + i,
                    'enum': 'TWO' if i % 2 else 'ONE',
                    'name': f'test{i}',
                }
                for i in range(10)
            ]
        )

        await client.execute(
            test_table.insert()
                .values([
                    {
                        'id': 20 + i,
                        'enum': 'TWO' if i % 2 else 'ONE',
                        'name': f'test{i}',
                    }
                    for i in range(10)
                ])
        )

        rows = await client.fetch(
            test_table.select()
                .where(test_table.c.enum == 'ONE')
                .order_by(test_table.c.id)
        )
        for row in rows:
            print(dict(row))


if __name__ == '__main__':
    asyncio.run(main())
