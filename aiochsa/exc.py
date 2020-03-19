import re


# Based on `getExceptionMessage()`:
# https://github.com/yandex/ClickHouse/blob/master/dbms/src/Common/Exception.cpp#L261
_match_exc_message = re.compile(
    r'^Code: (?P<code>\d+), '
        r'e\.displayText\(\) = (?P<display_text>.+?)'
        r'(?:, Stack trace[^:]*:\s+(?P<stack_trace>.+))?$',
    re.M,
).match


class AiochsaException(Exception):
    """ Base class for aiochsa exceptions """


class ProtocolError(AiochsaException):
    """ Error communicating to Clickhouse server """


class DBException(AiochsaException):
    """ Error returned from Clickhouse database """

    def __init__(self, statement, code, display_text, stack_trace=None):
        super().__init__(statement, code, display_text, stack_trace)
        self.statement = statement
        self.code = code
        self.display_text = display_text
        self.stack_trace = stack_trace

    def __str__(self):
        statement = self.statement
        if len(statement) > 200:
            statement = statement[:200] + '...'
        return f'[Code={self.code}] {self.display_text}: {statement}'

    @classmethod
    def from_message(cls, statement, exc_message):
        m = _match_exc_message(exc_message)
        if m:
            return cls(
                statement=statement,
                code=int(m.group('code')),
                display_text=m.group('display_text'),
                stack_trace=m.group('stack_trace'),
            )
        else: # pragma: nocover
            return cls(
                statement=statement, code=None, display_text=exc_message,
            )
