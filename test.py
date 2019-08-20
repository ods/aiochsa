#!/usr/bin/env python

import asyncio
from datetime import datetime
from decimal import Decimal
import re

from aiochclient import ChClient, ChClientError
from aiohttp import ClientSession
from clickhouse_sqlalchemy.drivers.http.base import dialect
from clickhouse_sqlalchemy.drivers.http.escaper import Escaper
import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.ddl import DDLElement


TEST_CREATE_DDL = '''\
CREATE TABLE test
(
    id UInt64,
    name String
    -- timestamp DateTime,
    -- amount Decimal64(9)
)
ENGINE = MergeTree()
ORDER BY id
'''

TEST_DROP_DDL = 'DROP TABLE test'


test_table = sa.Table(
    'test', sa.MetaData(),
    sa.Column('id', sa.Integer),
    sa.Column('name', sa.String),
    #sa.Column('timestamp', sa.DateTime),
    #sa.Column('amount', sa.DECIMAL),
)


RE_INSERT_VALUES = re.compile(
    r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s*)" +
        r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))" +
        r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)

_dialect = dialect()
_escaper = Escaper()


def execute_defaults(query):
    if isinstance(query, Insert):
        attr_name = 'default'
    elif isinstance(query, Update):
        attr_name = 'onupdate'
    else:
        return query

    # query.parameters could be a list in a multi row insert
    if isinstance(query.parameters, list):
        for param in query.parameters:
            _execute_default_attr(query, param, attr_name)
    else:
        query.parameters = query.parameters or {}
        _execute_default_attr(query, query.parameters, attr_name)
    return query

def _execute_default_attr(query, param, attr_name):
    # XXX Use _process_executesingle_defaults and _process_executemany_defaults
    # method of ExecutionContext? Or get_insert_default and get_update_default?
    # Created from dialect.execution_ctx_cls
    for col in query.table.columns:
        attr = getattr(col, attr_name)
        if attr and param.get(col.name) is None:
            if attr.is_sequence:
                param[col.name] = sa.func.nextval(attr.name)
            elif attr.is_scalar:
                param[col.name] = attr.arg
            elif attr.is_callable:
                param[col.name] = attr.arg({})


def compile_query(query, args):
    print([query, args])
    if isinstance(query, str):
        return query, args
    elif isinstance(query, DDLElement):
        compiled = query.compile(dialect=_dialect)
        assert not args
        return compiled.string % _escaper.escape(compiled.params), []
    elif isinstance(query, ClauseElement):
        query = execute_defaults(query)
        compiled = query.compile(dialect=_dialect)
        m = RE_INSERT_VALUES.match(compiled.string)
        if m:
            q_prefix = m.group(1) % ()
            q_values = m.group(2).rstrip()
            values_list = [
                q_values % _escaper.escape(parameters)
                for parameters in args or [compiled.params]
            ]
            query = '{} {};'.format(q_prefix, ','.join(values_list))
            return query, []
        else:
            assert not args
            query = compiled.string % _escaper.escape(compiled.params)
            return query, []
    else:
        assert False, type(query)


class ChClientSa(ChClient):

    async def _execute(self, query: str, *args):
        query, args = compile_query(query, args)
        print('_execute', query, args)
        async for rec in super()._execute(query, *args):
            yield rec


async def main():
    async with ClientSession() as s:
        client = ChClientSa(s, url='http://ch:8123')
        try:
            await client.execute(TEST_DROP_DDL)
        except ChClientError:
            pass
        await client.execute(TEST_CREATE_DDL)

        query = (
            test_table.insert()
                .values(
                    id=1,
                    name='test',
                    #timestamp=datetime.utcnow(),
                    #amount=Decimal('1.23'),
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


if __name__ == '__main__':
    asyncio.run(main())
