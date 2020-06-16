from clickhouse_sqlalchemy.drivers.base import ClickHouseCompiler
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http
from sqlalchemy import exc
from sqlalchemy.sql import crud
from sqlalchemy.sql.compiler import SQLCompiler

from .sql import _ORDER_BY_SENTINEL


class ClickhouseSaSQLCompiler(ClickHouseCompiler):

    _clickhouse_json_each_row = False

    def get_from_hint_text(self, table, text):
        return text

    def order_by_clause(self, select, **kw):
        text = ''
        if select._order_by_clause is not _ORDER_BY_SENTINEL:
            text = super().order_by_clause(select, **kw)

        # Hack to add LIMIT BY clause
        limit_by_clause = getattr(select, '_limit_by_clause', None)
        if limit_by_clause is not None and limit_by_clause.clauses.clauses:
            text += ' LIMIT '
            if limit_by_clause.offset is not None:
                text += f'{self.process(limit_by_clause.offset, **kw)}, '
            text += self.process(limit_by_clause.limit, **kw)
            limit_by_exprs = limit_by_clause.clauses._compiler_dispatch(
                self, **kw,
            )
            text += f' BY {limit_by_exprs}'

        return text

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        assert not self.stack # INSERT only at top level

        self.stack.append(
            {
                "correlate_froms": set(),
                "asfrom_froms": set(),
                "selectable": insert_stmt,
            }
        )

        preparer = self.preparer

        text = "INSERT INTO "
        table_text = preparer.format_table(insert_stmt.table)
        text += table_text

        if insert_stmt.select is not None:
            # `_setup_crud_params()` multiplies parameter placeholders for
            # multiparam inserts.  We don't want this, so this part is moved to
            # the branch for `INSERT INTO ... SELECT`.
            crud_params = crud._setup_crud_params(
                self, insert_stmt, crud.ISINSERT, **kw
            )

            if not crud_params: # pragma: no cover
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support empty "
                    "inserts." % self.dialect.name
                )

            assert not insert_stmt._has_multi_parameters

            text += " (%s)" % ", ".join(
                [preparer.format_column(c[0]) for c in crud_params]
            )

            select_text = self.process(self._insert_from_select, **kw)

            # TODO Provide visit_cte for Clickhouse variant of CTE
            if self.ctes:
                text += " %s%s" % (self._render_cte_clause(), select_text)
            else:
                text += " %s" % select_text
        else:
            # This is normally done by `crud._setup_crud_params()`
            self.isinsert = True

            self._clickhouse_json_each_row = True
            text += ' FORMAT JSONEachRow'

        assert insert_stmt._post_values_clause is None

        self.stack.pop(-1)

        assert not asfrom
        return text


class ClickhouseSaDialect(ClickHouseDialect_http):
    statement_compiler = ClickhouseSaSQLCompiler

    supports_empty_insert = False
    cte_follows_insert = True
