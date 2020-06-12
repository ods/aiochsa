from types import SimpleNamespace

from clickhouse_sqlalchemy.sql import Select as _BaseSelect
from sqlalchemy.sql.base import _generative
from sqlalchemy.sql.elements import ClauseList, _literal_as_label_reference
from sqlalchemy.sql.selectable import _offset_or_limit_clause


class LimitByClause:

    def __init__(self, clauses, offset, limit):
        self.clauses = ClauseList(
            *clauses, _literal_as_text=_literal_as_label_reference,
        )
        self.offset = _offset_or_limit_clause(offset)
        self.limit = _offset_or_limit_clause(limit)



# Minimal value to trick SQLAlchemy into believing that there is a ORDER BY
# clause
_ORDER_BY_SENTINEL = SimpleNamespace(clauses=True)


class Select(_BaseSelect):

    _limit_by_clause = None

    @_generative
    def limit_by(self, *clauses, offset=None, limit):
        """return a new selectable with the given
        `LIMIT [offset,] limit BY clauses` criterion applied.

        e.g.::

            stmt = (
                select([table.c.name, table.c.amount])
                    .order_by(table.c.amount.desc())
                    .limit_by(table.c.name, limit=1)
            )
        """
        # LIMIT BY clause comes just after ORDER BY, so we hack the later's
        # machinery to add former too.  Alternative would be redefining
        # whole `_compose_select_body` method just to insert 2 lines of code.
        # The worst here is that this method is redefined in
        # clickhouse_sqlalchemy and still is under active development.
        if not self._order_by_clause.clauses:
            self._order_by_clause = _ORDER_BY_SENTINEL

        self._limit_by_clause = LimitByClause(clauses, offset, limit)

    def append_order_by(self, *clauses):
        if self._order_by_clause is _ORDER_BY_SENTINEL:
            self._order_by_clause = ClauseList()
        super().append_order_by(*clauses)
        if (
            self._limit_by_clause is not None and
            not self._order_by_clause.clauses
        ):
            self._order_by_clause = _ORDER_BY_SENTINEL


select = Select
