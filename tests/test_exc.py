import pytest

import aiochsa
from aiochsa import error_codes


async def test_exc(conn):
    with pytest.raises(aiochsa.DBException) as exc_info:
        await conn.execute('ERROR')

    assert exc_info.value.code == error_codes.SYNTAX_ERROR
    assert 'Syntax error' in exc_info.value.display_text

    assert str(error_codes.SYNTAX_ERROR) in str(exc_info.value)
    assert 'Syntax error' in str(exc_info.value)


async def test_exc_stacktrace(dsn):
    async with aiochsa.connect(dsn, stacktrace=1) as conn:
        with pytest.raises(aiochsa.DBException) as exc_info:
            await conn.execute('ERROR')

    assert exc_info.value.code == error_codes.SYNTAX_ERROR
    assert 'Syntax error' in exc_info.value.display_text
    assert exc_info.value.stack_trace is not None
