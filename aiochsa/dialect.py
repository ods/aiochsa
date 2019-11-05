from clickhouse_sqlalchemy.drivers.base import ClickHouseCompiler
from clickhouse_sqlalchemy.drivers.http.base import ClickHouseDialect_http
from sqlalchemy import exc
from sqlalchemy.sql import crud


class ClickhouseSaSQLCompiler(ClickHouseCompiler):

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        assert not self.stack # INSERT only at top level

        self.stack.append(
            {
                "correlate_froms": set(),
                "asfrom_froms": set(),
                "selectable": insert_stmt,
            }
        )

        crud_params = crud._setup_crud_params(
            self, insert_stmt, crud.ISINSERT, **kw
        )

        if not crud_params: # pragma: no cover
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support empty "
                "inserts." % self.dialect.name
            )

        if insert_stmt._has_multi_parameters:
            crud_params_single = crud_params[0]
        else:
            crud_params_single = crud_params

        preparer = self.preparer

        text = "INSERT INTO "
        table_text = preparer.format_table(insert_stmt.table)
        text += table_text

        text += " (%s)" % ", ".join(
            [preparer.format_column(c[0]) for c in crud_params_single]
        )

        if insert_stmt.select is not None:
            select_text = self.process(self._insert_from_select, **kw)

            # TODO Provide visit_cte for Clickhouse variant of CTE
            if self.ctes:
                text += " %s%s" % (self._render_cte_clause(), select_text)
            else:
                text += " %s" % select_text
        elif insert_stmt._has_multi_parameters:
            text += " VALUES %s" % (
                ", ".join(
                    "(%s)" % (", ".join(c[1] for c in crud_param_set))
                    for crud_param_set in crud_params
                )
            )
        else:
            insert_single_values_expr = ", ".join([c[1] for c in crud_params])
            text += " VALUES (%s)" % insert_single_values_expr
            self.insert_single_values_expr = insert_single_values_expr

        assert insert_stmt._post_values_clause is None

        self.stack.pop(-1)

        assert not asfrom
        return text


class ClickhouseSaDialect(ClickHouseDialect_http):
    statement_compiler = ClickhouseSaSQLCompiler

    supports_empty_insert = False
    cte_follows_insert = True
