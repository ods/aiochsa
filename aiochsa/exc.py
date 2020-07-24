from collections import namedtuple
import re


# Based on `getExceptionMessage()`:
# https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Exception.cpp#L370-L373
exc_message_re = re.compile(
    r'Code: (?P<code>\d+), '
        r'e\.displayText\(\) = (?P<display_text>.+?)'
        r'(?:, Stack trace[^:]*:\s+(?P<stack_trace>.+))?$',
    re.M,
)

at_row_re = re.compile(r'[(]at row (?P<num>\d+)[)]')

class AiochsaException(Exception):
    """ Base class for aiochsa exceptions """


class ProtocolError(AiochsaException):
    """ Error communicating to Clickhouse server """


RowInfo = namedtuple('RowInfo', ['num', 'content'])


class DBException(AiochsaException):
    """ Error returned from Clickhouse database """

    def __init__(
        self, code, display_text, stack_trace=None, statement=None, row=None,
    ):
        super().__init__(code, display_text, stack_trace)
        self.code = code
        self.display_text = display_text
        self.stack_trace = stack_trace
        self.statement = statement
        self.row = row

    def __str__(self):
        message = f'[Code={self.code}] {self.display_text}'
        if self.statement:
            statement = self.statement
            if len(statement) > 200:
                statement = statement[:200] + '...'
            message += f'\n{statement}'
        if self.row:
            message += f'\n{self.row.num}: {self.row.content}'
        return message

    @classmethod
    def from_message(cls, exc_message, *, statement=None, rows=None):
        m = exc_message_re.match(exc_message)
        if m:
            display_text = m.group('display_text')

            row = None
            if rows:
                at_row_m = at_row_re.search(display_text)
                if at_row_m:
                    # It's 1-based
                    row_num = int(at_row_m.group('num'))
                    if len(rows) >= row_num:
                        row = RowInfo(row_num, rows[row_num - 1])

            return cls(
                code=int(m.group('code')),
                display_text=display_text,
                stack_trace=m.group('stack_trace'),
                statement=statement,
                row=row,
            )
        else: # pragma: nocover
            return cls(
                code=None, display_text=exc_message, statement=statement,
            )
