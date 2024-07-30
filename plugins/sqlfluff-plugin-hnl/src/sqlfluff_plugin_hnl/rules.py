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
from sqlfluff.utils.reflow import ReflowSequence


class Rule_HNL_A001(BaseRule):
    """
    Column aliases should always be used or 
    used consistently within a file or clause, 
    based on the configuration.

    ### Anti-pattern

    Using an alias for one column in a SELECT 
    clause but not using an alias for another 
    column in the same clause or file.

    .. code-block:: sql

        SELECT
            col AS col,
            col
        FROM table;

    ### Best Practices

    1. **Always Use Alias**: Aliases should always be used for all columns.

        .. code-block:: sql

            SELECT
                col AS col,
                col AS col
            FROM table;

    2. **Consistent within Clause**: Aliases should be consistent within the same clause.

        .. code-block:: sql

            WITH cte AS (
                SELECT
                    col AS col,
                    col AS col
                FROM table
            )
            SELECT
                col,
                col
            FROM cte;

    3. **Consistent within File**: Aliases should be consistent within the entire file.

        .. code-block:: sql

            WITH cte AS (
                SELECT
                    col AS col,
                    col AS col
                FROM table
            )
            SELECT
                col AS col,
                col AS col
            FROM cte;

        .. code-block:: sql
    """

    groups = ("all", "aliasing")
    crawl_behaviour = SegmentSeekerCrawler({"select_clause"})
    config_keywords = ["alias_usage_style"]
    is_fix_compatible = True

    def _eval(self, context: RuleContext) -> EvalResultType:
        """
        Find columns that, depending on the configuration,
        either do not have an alias (in case of configuration always)
        or are not consistent with the first occurrence (in case of
        configuration clause or file consistent).

        Columns that are simple, non-expression,
        reference columns, which do not have an alias but should have
        an alias, are fixed by adding the `AS` keyword and 
        the last identifier of the column is added as the alias.
        For example: `a.col_a` is fixed to `a.col_a AS col_a`.

        If the column has an alias but should not have an alias, 
        the alias is removed.
        For example: `a.col_a AS col_a` is fixed to `a.col_a`.
        """
        assert context.segment.is_type("select_clause")

        self.alias_usage_style: str
        violations = []
        children: Segments = FunctionalContext(context).segment.children()

        # Use memory to store decuded alias usage for when usage should be cosistent in entire file
        memory = context.memory
        alias_usage = None
        if(self.alias_usage_style == "consistent_file"):
            alias_usage = context.memory.get("alias_usage")
        elif(self.alias_usage_style == "always"):
            alias_usage = "yes"

        for clause_element in children.select(sp.is_type("select_clause_element")):
            # Get alias of select clause element, if it exists
            alias_expression = clause_element.get_child("alias_expression")  # `as col_a`

            # Set the deduced alias usage based on whether the first occurrence in the select clause has an alias
            if not alias_usage:
                alias_usage = (
                    "yes" if alias_expression else "no"
                )
                memory["alias_usage"] = alias_usage

            # When column does not deviate from expected alias usage go to next column in select clause
            if alias_usage == "yes" and alias_expression or alias_usage == "no" and not alias_expression: 
                continue

            # Continue when we have a deviation from the expected alias usage
            fixes = []
  
            # Get referenced column for simple columns, if it exists
            column_reference = clause_element.get_child("column_reference")

            # We can only add an alias to simple columns. For these, we will use
            # the name of the referenced column as the alias (self-alias).
            # For complex or expression columns, the user must decide the alias.
            if(alias_usage == "yes" and column_reference):
                # If column has either a naked_identifier or quoted_identifier
                # (not positional identifier like $n in snowflake)
                # then continue
                if column_reference.get_child("naked_identifier") or column_reference.get_child(
                    "quoted_identifier"
                ):            
                    # If the column name is quoted then get the `quoted_identifier`,
                    # otherwise get the last `naked_identifier`.
                    # The last naked_identifier in column_reference type
                    # belongs to the column name.
                    # Example: a.col_name where `a` is table name/alias identifier
                    if column_reference.get_child("quoted_identifier"):
                        column_identifier = column_reference.get_child("quoted_identifier")
                    else:
                        column_identifier = column_reference.get_children("naked_identifier")[-1]     

                    fixes = [
                        LintFix.create_after(
                            column_reference,
                            [WhitespaceSegment(), KeywordSegment("AS"), WhitespaceSegment(), column_identifier],
                        )
                    ]
            elif(alias_usage == "no"):
                if alias_expression:
                    fixes = [
                        LintFix.delete(
                            alias_expression
                        )
                    ]                

            usage_styles = {
                "consistent_file": "file",
                "consistent_clause": "clause"
            }

            # Determine the description based on alias_usage_style
            if self.alias_usage_style in usage_styles:
                usage_style = usage_styles[self.alias_usage_style]
                description = f"Column alias usage should be consistent within {usage_style}."
            elif self.alias_usage_style == "always":
                description = "Column should always use an alias."

            violations.append(
                LintResult(
                    anchor=clause_element,
                    description=description,
                    fixes=fixes,
                    memory=memory,
                )
            )   

        return violations or None
class Rule_HNL_A002(BaseRule):
    """
    Columns should be ordered in SELECT clauses 
    according to a specific convention to improve 
    the findability of columns during debugging.

    ### Ordering Convention

    1. Business Keys (columns starting with `BK`)
    2. Technical Keys (columns ending with `ID`)
    3. SourceSystem dimension business key and 
    technical key (columns starting with `BKSourceSystem` 
    and columns ending with `SourceSystemID`)
    4. All other columns
    5. SourceSystemCode column

    If multiple columns fall within the same category, 
    they should be ordered alphabetically.

    ### Anti-pattern

    Incorrectly ordered columns, such as placing 
    measure columns before business key columns.

    .. code-block:: sql

        SELECT
            AmountInvoiced,
            SourceSystemCode,
            InvoicedDate,
            BKSourceSystem,
            BKInvoicedDate
        FROM table;

    ### Best Practice

    Use the correct column order as per the 
    convention, with business keys preceding 
    measure columns in a fact table.

    .. code-block:: sql

        SELECT
            BKInvoicedDate,
            BKSourceSystem,
            AmountInvoiced,
            InvoicedDate,
            SourceSystemCode
        FROM table;
    """


    groups = ("all", "aliasing")
    crawl_behaviour = SegmentSeekerCrawler({"select_clause"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> EvalResultType:
        """
        Identify columns that are out of order a
        ccording to the specified ordering scheme.

        First, a list of all columns is compiled. 
        Columns with a  are excluded 
        to allow for deviations from the 
        convention for specific reasons.

        The list is then ordered according to 
        the convention. A violation is triggered 
        for any column that appears at a different 
        index than its position in the ordered list.
        """
        assert context.segment.is_type("select_clause")       

        identifiers = []
        violations = []
        children: Segments = FunctionalContext(context).segment.children()

        # Get lines that contain a comment that explicitly exclude columns
        # on that line from the ordering scheme check
        # We take all comments from the parent since a comment at the end
        # of a SELECT clause is apparently part of the parent segement
        line_positions_to_ignore = []
        comments = context.segment.get_parent()[0].recursive_crawl("comment")
        for comment in comments:
            if comment.raw in ("-- noqa", "-- noqa: HNL_A002"):
                line_positions_to_ignore.append(comment.get_start_loc()[0])

        for clause_element in children.select(sp.is_type("select_clause_element")):
            # Get alias of select clause element, if it exists
            column = clause_element.get_child("column_reference")  # `col_a`
            alias_expression = clause_element.get_child("alias_expression")  # `as col_a`

            identifier = None
            if alias_expression:
                identifier = alias_expression.get_child(
                    "naked_identifier"
                ) or alias_expression.get_child("quoted_identifier")                
            elif column:
                if column.get_child("naked_identifier") or column.get_child(
                    "quoted_identifier"
                ):                
                    if column.get_child("quoted_identifier"):
                        identifier = column.get_child("quoted_identifier")
                    else:
                        identifier = column.get_children("naked_identifier")[-1]

            if not identifier:
                continue

            identifier = identifier.raw.strip("\"'`[]")

            # Only add identifiers that are not on a line that contains a `noqa` directive
            if clause_element.get_start_loc()[0] not in line_positions_to_ignore:
                identifiers.append((identifier, clause_element))

        # Sorting the list based on our ordering scheme
        sorted_identifiers = sorted(identifiers, key=custom_sort_key)

        # Find elements that have changed their position
        changed_identifiers = [original for original, sorted_ in zip(identifiers, sorted_identifiers) if original != sorted_]

        for _, clause_element in changed_identifiers:
            violations.append(
                LintResult(
                    anchor=clause_element,
                    description=f"Column should be ordered based on ordering scheme.",
                )
            )

        return violations

def custom_sort_key(item):
    raw_string, _ = item

    # Define sort priority based on conditions
    if raw_string.startswith("BK"):
        priority = 0
    elif raw_string.endswith("ID"):
        priority = 1
    elif raw_string in ("BKSourceSystem", "SourceSystemID"):
        priority = 2
    elif raw_string == "SourceSystemCode":
        priority = 4
    else:
        priority = 3

    return (priority, raw_string.lower())        