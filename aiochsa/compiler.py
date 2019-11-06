from types import SimpleNamespace

from sqlalchemy.engine.util import _distill_params
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
        distilled_params = _distill_params(multiparams, {})
        print(f'distilled_params={distilled_params}')
        init_compiled_parameters = ()
        if distilled_params:
            if isinstance(elem, Insert):
                pass#elem = elem.values(distilled_params)
            else:
                # For now we support passing parameters separate from
                # statement for INSERTs only, so we have to inline them into
                # statement.
                # FIXME Implement Clickhouse's paramstyle and pass parameters
                # in the body of request:
                # https://clickhouse.yandex/docs/en/interfaces/http/#cli-queries-with-parameters
                #elem = elem.params(*distilled_params)
                init_compiled_parameters = distilled_params
                #distilled_params = ()
        compiled_sql = elem.compile(
            dialect=self._dialect,
            inline=True, # Never add constructs to return default values
        )
        return self._execute_context(
            self._dialect,
            self._dialect.execution_ctx_cls._init_compiled,
            compiled_sql,
            distilled_params,
            # The rest are `*args` to `_init_compiled`
            compiled_sql,
            init_compiled_parameters,
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

        # Only SQL compiler has this attribute, but not DDL compiler
        if getattr(statement, '_clickhouse_json_each_row', False):
            print(f'parameters={parameters}')
            print(f'context.parameters={context.parameters}')
            # We can't use `context.parameters` here, since we trick
            # `_init_compiled()` to think we have no parameters, so it's always
            # empty here.
            return context.statement, parameters or context.parameters
        else:
            assert len(context.parameters) == 1
            escaped = {
                name: self._encode(value)
                for name, value in context.parameters[0].items()
            }
            return context.statement % escaped, ()


    def compile_statement(self, statement, args):
        if isinstance(statement, str):
            assert not args
            return statement, args
        elif isinstance(statement, ClauseElement):
            if isinstance(statement, DDLElement):
                statement, parameters = self._execute_ddl(statement, args)
            elif isinstance(statement, FunctionElement):
                statement, parameters = self._execute_function(statement, args)
            else:
                statement, parameters = self._execute_clauseelement(
                    statement, args,
                )
            return statement, parameters
        else:
            raise TypeError(f'Execution of {type(statement)} is not supported')
