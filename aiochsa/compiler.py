from types import SimpleNamespace

from sqlalchemy.sql import func, ClauseElement
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.sql.dml import Insert
from sqlalchemy.sql.functions import FunctionElement


class Compiler:

    def __init__(self, dialect, encode):
        self._dialect = dialect
        self._encode = encode

    def _execute_clauseelement(self, elem, multiparams):
        # Modeled after `sqlalchemy.engine.base.Connection._execute_clauseelement`
        # (event signaling, caching are removed; separate parameters are merge into
        # clause element)
        if multiparams:
            # Clickhouse doesn't support passing parameters separate from
            # statement, so we have to inline them into statement.
            if len(multiparams) == 1 and isinstance(multiparams[0], dict):
                multiparams = multiparams[0]
            if isinstance(elem, Insert):
                elem = elem.values(multiparams)
            else:
                elem = elem.params(multiparams)
        compiled_sql = elem.compile(
            dialect=self._dialect,
        )
        return self._execute_context(
            self._dialect,
            self._dialect.execution_ctx_cls._init_compiled,
            compiled_sql,
            (),
            compiled_sql,
            (),
        )

    def _execute_function(self, func, multiparams):
        # Modeled after `sqlalchemy.engine.base.Connection._execute_function`
        return self._execute_clauseelement(func.select(), multiparams)

    def _execute_ddl(self, ddl, multiparams):
        # Modeled after `sqlalchemy.engine.base.Connection._execute_ddl` (event
        # signaling is removed).
        compiled = ddl.compile(dialect=self._dialect)
        return self._execute_context(
            self._dialect,
            self._dialect.execution_ctx_cls._init_ddl,
            compiled,
            None,
            compiled,
        )

    def _execute_context(self, dialect, constructor, statement, parameters, *args):
        # Modeled after `sqlalchemy.engine.base.Connection._execute_context` (event
        # signaling is removed; return statement as string instead of dispatching
        # to `dialect.{do_execute,do_executemany,do_execute_no_params}`; no
        # exception convertion).
        conn = SimpleNamespace(dialect=dialect, _execution_options={})
        db_api_conn = SimpleNamespace(cursor=lambda: None)
        context = constructor(dialect, conn, db_api_conn, *args)
        assert len(context.parameters) == 1
        escaped = {
            name: self._encode(value)
            for name, value in context.parameters[0].items()
        }
        return context.statement % escaped


    def compile_statement(self, statement, args):
        if isinstance(statement, str):
            assert not args
            return statement
        elif isinstance(statement, ClauseElement):
            if isinstance(statement, DDLElement):
                statement = self._execute_ddl(statement, args)
            elif isinstance(statement, FunctionElement):
                statement = self._execute_function(statement, args)
            else:
                statement = self._execute_clauseelement(statement, args)
            return statement
        else:
            raise TypeError(f'Execution of {type(statement)} is not supported')
