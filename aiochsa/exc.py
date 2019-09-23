import re


# Based on `getExceptionMessage()`:
# https://github.com/yandex/ClickHouse/blob/master/dbms/src/Common/Exception.cpp#L261
_match_exc_message = re.compile(
    r'^Code: (?P<code>\d+), '
        r'e\.displayText\(\) = (?P<display_text>.+?)'
        r'(?:, Stack trace:\s+(?P<stack_trace>.+))?$',
    re.M,
).match


class DBException(Exception):

    def __init__(self, code, display_text, stack_trace=None):
        super().__init__(code, display_text, stack_trace)
        self.code = code
        self.display_text = display_text
        self.stack_trace = stack_trace

    def __str__(self):
        return f'[Code={self.code}] {self.display_text}'

    @classmethod
    def from_message(cls, exc_message):
        m = _match_exc_message(exc_message)
        if m:
            return cls(
                code=int(m.group('code')),
                display_text=m.group('display_text'),
                stack_trace=m.group('stack_trace'),
            )
        else: # pragma: nocover
            return cls(code=None, display_text=exc_message)
