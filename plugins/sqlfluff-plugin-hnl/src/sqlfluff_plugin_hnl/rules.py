"""HNL custom rules implemented through the plugin system.

This uses the rules API supported from 0.4.0 onwards.
"""
from sqlfluff.core.rules import (
    BaseRule,
    LintResult,
    RuleContext,
    LintFix,
)
from sqlfluff.core.rules.base import EvalResultType
from sqlfluff.core.rules.context import RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.utils.functional import FunctionalContext, Segments, sp
from sqlfluff.core.parser import KeywordSegment, WhitespaceSegment


class Rule_HNL_A001(BaseRule):
    """Simple (non-expression) column should have alias

    Even when a column is a simple column,
    i.e. it does not have an expression and
    is only a reference to a column, it should
    have an alias (self-alias)

    **Anti-pattern**

    To not use an alias on a simple column.

    .. code-block:: sql

        SELECT
            col
        FROM table;

    **Best practice**

    To use self-alias on a simple column.

    .. code-block:: sql

        SELECT
            col AS col
        FROM table;
    """

    groups = ("all", "aliasing")
    crawl_behaviour = SegmentSeekerCrawler({"select_clause"})
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> EvalResultType:
        """Find simple columns without alias and fix them.

        Checks whether the column is a simple, non-expression, 
        reference column and checks whether it has an 
        alias (self-alias).

        If the column is simple and does not have an alias,
        then the `AS` keyword and the last identiefier of the column
        is added as the alias.
        For example: `a.col_a,` is fixed to `a.col_a AS col_a,`
        """
        assert context.segment.is_type("select_clause")

        violations = []

        children: Segments = FunctionalContext(context).segment.children()

        for clause_element in children.select(sp.is_type("select_clause_element")):
            clause_element_raw_segments = (
                clause_element.get_raw_segments()
            )  # col_a as col_a

            column = clause_element.get_child("column_reference")  # `col_a`
            alias_expression = clause_element.get_child(
                "alias_expression"
            )  # `as col_a`

            # If the alias is for a column_reference type (not function)
            # then continue
            if column and not alias_expression:
                # If column has either a naked_identifier or quoted_identifier
                # (not positional identifier like $n in snowflake)
                # then continue
                if column.get_child("naked_identifier") or column.get_child(
                    "quoted_identifier"
                ):
                    # If the column name is quoted then get the `quoted_identifier`,
                    # otherwise get the last `naked_identifier`.
                    # The last naked_identifier in column_reference type
                    # belongs to the column name.
                    # Example: a.col_name where `a` is table name/alias identifier
                    if column.get_child("quoted_identifier"):
                        column_identifier = column.get_child("quoted_identifier")
                    else:
                        column_identifier = column.get_children("naked_identifier")[-1]

                    assert column_identifier

                    # Column not self-aliased
                    violations.append(
                        LintResult(
                            anchor=clause_element_raw_segments[0],
                            description="Column should be aliased.",
                            fixes=[
                                LintFix.create_after(
                                    clause_element_raw_segments[-1],
                                    [WhitespaceSegment(), KeywordSegment("AS"), WhitespaceSegment(), column_identifier],
                                ),
                            ]
                        )
                    )           

        return violations or None