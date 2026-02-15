"""
SQL Compiler Module
Translates DSL to DuckDB SQL with proper CTE chaining and value escaping
"""

from datetime import datetime
from typing import Any, Dict, List

from src.models.dsl import (
    AggregationAssertion,
    AggregationSumAssertion,
    ColumnComparisonAssertion,
    EnterpriseControlDSL,
    FilterComparison,
    FilterInList,
    FilterIsNull,
    TemporalDateMathAssertion,
    TemporalSequenceAssertion,
    ValueMatchAssertion,
)
from src.utils.logging_config import get_logger

# Get logger for this module
logger = get_logger(__name__)


class ControlCompiler:
    """Compiles DSL into DuckDB SQL with exception detection logic"""

    def __init__(self, dsl: EnterpriseControlDSL):
        logger.debug(f"Initializing ControlCompiler for {dsl.governance.control_id}")
        self.dsl = dsl
        self.population_filters: List[str] = []  # MUST be true (AND)
        self.assertion_exceptions: List[
            str
        ] = []  # If ANY are true, it's a failure (OR)
        self.having_conditions: List[str] = []
        self.group_by_fields: List[str] = []
        self.cte_fragments: List[str] = []

    @staticmethod
    def _normalize_field_name(field: str) -> str:
        """
        Strip dataset prefix from field names.
        Example: 'wall_cross_register_sheet1.employee_id' -> 'employee_id'

        This is necessary because after joins, columns are accessed by their base name,
        not dataset.column notation.
        """
        if "." in field:
            # Return everything after the last dot
            return field.split(".")[-1]
        return field

    def compile_to_sql(self, parquet_manifests: Dict[str, Dict[str, Any]]) -> str:
        """
        Generates SQL that returns EXCEPTIONS (rows violating rules).

        Args:
            parquet_manifests: Maps dataset_alias to {'parquet_path': str, 'sha256_hash': str}

        Returns:
            DuckDB SQL query string
        """
        # Step 1: Build population CTE with proper chaining
        final_population_alias = self._build_population_cte(parquet_manifests)

        # Step 2: Route assertions to WHERE or HAVING clauses
        self._compile_assertions()

        # Step 3: Apply sampling if configured
        sampling_clause = self._build_sampling_clause()

        # Step 4: Construct final query
        return self._construct_query(final_population_alias, sampling_clause)

    def _build_population_cte(self, manifests: Dict[str, Dict[str, Any]]) -> str:
        """
        Builds CTE chain with filters and joins.
        Returns the final CTE alias to use in SELECT.
        """
        base_alias = self.dsl.population.base_dataset
        base_path = manifests[base_alias]["parquet_path"]

        # Start with base dataset
        current_cte = f"base AS (SELECT * FROM read_parquet('{base_path}'))"
        self.cte_fragments.append(current_cte)

        # Track the previous CTE alias for proper chaining (CRITICAL FIX)
        previous_alias = "base"

        # Apply pipeline steps
        for step in self.dsl.population.steps:
            action = step.action

            if action.operation == "filter_comparison":
                # Filters are applied in WHERE clause, not in CTE
                cond = self._compile_filter_comparison(action)
                self.population_filters.append(cond)

            elif action.operation == "filter_in_list":
                cond = self._compile_filter_in_list(action)
                self.population_filters.append(cond)

            elif action.operation == "filter_is_null":
                cond = self._compile_filter_is_null(action)
                self.population_filters.append(cond)

            elif action.operation == "join_left":
                right_path = manifests[action.right_dataset]["parquet_path"]

                # Build composite join conditions (supports multiple keys)
                join_conditions = []
                for l_key, r_key in zip(action.left_keys, action.right_keys):
                    join_conditions.append(
                        f"{previous_alias}.{l_key} = right_tbl.{r_key}"
                    )

                on_clause = " AND ".join(join_conditions)

                # CRITICAL FIX: Use DuckDB's EXCLUDE to prevent Ambiguous Column crashes
                # When both tables have the same column names (like join keys), we exclude
                # the duplicate columns from the right table to avoid SQL errors
                exclude_keys = ", ".join(action.right_keys)

                join_cte = f"""{step.step_id} AS (
    SELECT {previous_alias}.*,
           right_tbl.* EXCLUDE ({exclude_keys})
    FROM {previous_alias}
    LEFT JOIN read_parquet('{right_path}') AS right_tbl
    ON {on_clause}
)"""
                self.cte_fragments.append(join_cte)
                # Update the pointer to current step for next iteration
                previous_alias = step.step_id

        return previous_alias

    def _compile_filter_comparison(self, action: FilterComparison) -> str:
        """Compiles FilterComparison to SQL condition"""
        op_map = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
        }
        sql_op = op_map[action.operator]
        field = self._normalize_field_name(action.field)
        return f"{field} {sql_op} {self._quote_value(action.value)}"

    def _compile_filter_in_list(self, action: FilterInList) -> str:
        """Compiles FilterInList to SQL IN condition"""
        field = self._normalize_field_name(action.field)
        values_str = ", ".join([self._quote_value(v) for v in action.values])
        return f"{field} IN ({values_str})"

    def _compile_filter_is_null(self, action: FilterIsNull) -> str:
        """Compiles FilterIsNull to SQL IS NULL condition"""
        field = self._normalize_field_name(action.field)
        if action.is_null:
            return f"{field} IS NULL"
        else:
            return f"{field} IS NOT NULL"

    def _compile_assertions(self) -> None:
        """Routes assertions to appropriate SQL clauses"""
        for assertion in self.dsl.assertions:
            if isinstance(assertion, ValueMatchAssertion):
                # Row-level assertion → WHERE clause
                cond = self._compile_value_match(assertion)
                # Wrap in NOT to find exceptions
                self.assertion_exceptions.append(f"NOT ({cond})")

            elif isinstance(assertion, TemporalSequenceAssertion):
                # Temporal sequence → WHERE clause
                cond = self._compile_temporal_sequence(assertion)
                self.assertion_exceptions.append(f"NOT ({cond})")

            elif isinstance(assertion, TemporalDateMathAssertion):
                # Temporal date math → WHERE clause
                cond = self._compile_temporal_date_math(assertion)
                self.assertion_exceptions.append(f"NOT ({cond})")

            elif isinstance(assertion, ColumnComparisonAssertion):
                # Column-to-column comparison → WHERE clause
                cond = self._compile_column_comparison(assertion)
                self.assertion_exceptions.append(f"NOT ({cond})")

            elif isinstance(assertion, (AggregationSumAssertion, AggregationAssertion)):
                # Aggregation assertion → HAVING clause
                cond = self._compile_aggregation(assertion)
                self.having_conditions.append(f"NOT ({cond})")
                # Normalize group_by_fields to strip dataset prefixes
                normalized_fields = [
                    self._normalize_field_name(f) for f in assertion.group_by_fields
                ]
                self.group_by_fields.extend(normalized_fields)

    def _compile_value_match(self, assertion: ValueMatchAssertion) -> str:
        """Translates ValueMatchAssertion to SQL condition"""
        field = self._normalize_field_name(assertion.field)
        operator = assertion.operator
        value = assertion.expected_value

        # CRITICAL FIX: Handle SQL NULL semantics
        if value is None:
            if operator == "eq":
                return f"{field} IS NULL"
            elif operator == "neq":
                return f"{field} IS NOT NULL"
            else:
                raise ValueError(
                    f"Operator {operator} invalid for NULL comparison. Use 'eq' for IS NULL or 'neq' for IS NOT NULL."
                )

        # Map DSL operators to SQL operators
        op_map = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
            "in": "IN",
            "not_in": "NOT IN",
        }

        sql_op = op_map[operator]

        # Handle list values for IN operator
        if operator in ["in", "not_in"]:
            if isinstance(value, list):
                values_str = ", ".join([self._quote_value(v) for v in value])
                return f"{field} {sql_op} ({values_str})"

        # CRITICAL FIX: Handle case-insensitive string comparison
        if (
            getattr(assertion, "ignore_case_and_space", True)
            and isinstance(value, str)
            and operator not in ["in", "not_in"]
        ):
            # Trim and uppercase both sides for string comparisons
            return f"TRIM(UPPER(CAST({field} AS VARCHAR))) {sql_op} TRIM(UPPER({self._quote_value(value)}))"

        # Handle scalar values
        return f"{field} {sql_op} {self._quote_value(value)}"

    def _compile_temporal_sequence(self, assertion: TemporalSequenceAssertion) -> str:
        """Translates TemporalSequenceAssertion to SQL condition"""
        # Build chain: event1 < event2 < event3 ...
        conditions = []
        for i in range(len(assertion.event_chain) - 1):
            event1 = self._normalize_field_name(assertion.event_chain[i])
            event2 = self._normalize_field_name(assertion.event_chain[i + 1])
            conditions.append(f"{event1} < {event2}")

        return " AND ".join(conditions)

    def _compile_temporal_date_math(self, assertion: TemporalDateMathAssertion) -> str:
        """Translates TemporalDateMathAssertion to SQL with INTERVAL arithmetic"""
        op_map = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}
        sql_op = op_map[assertion.operator]

        # CRITICAL FIX: Cast date fields to DATE type to handle VARCHAR/string dates
        # Also normalize field names to strip dataset prefixes
        base_field = self._normalize_field_name(assertion.base_date_field)
        target_field = self._normalize_field_name(assertion.target_date_field)
        # Translates to: CAST(edd_date AS DATE) <= CAST(onboarding_date AS DATE) + INTERVAL 14 DAY
        return f"CAST({base_field} AS DATE) {sql_op} CAST({target_field} AS DATE) + INTERVAL {assertion.offset_days} DAY"

    def _compile_column_comparison(self, assertion: ColumnComparisonAssertion) -> str:
        """Translates ColumnComparisonAssertion to SQL (compares two columns)"""
        op_map = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
        }
        sql_op = op_map[assertion.operator]

        # Normalize both field names to strip dataset prefixes
        left_field = self._normalize_field_name(assertion.left_field)
        right_field = self._normalize_field_name(assertion.right_field)
        # Notice we DO NOT use self._quote_value() here, because right_field is a column name!
        return f"{left_field} {sql_op} {right_field}"

    def _compile_aggregation(self, assertion) -> str:
        """Translates Aggregation assertions to SQL HAVING condition"""
        metric = self._normalize_field_name(assertion.metric_field)
        operator = assertion.operator
        threshold = assertion.threshold

        op_map = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}
        sql_op = op_map[operator]

        # Determine aggregation function
        if isinstance(assertion, AggregationAssertion):
            agg_func = assertion.aggregation_function
        else:
            # Backward compatibility for AggregationSumAssertion
            agg_func = "SUM"

        return f"{agg_func}({metric}) {sql_op} {threshold}"

    def _build_sampling_clause(self) -> str:
        """Builds DuckDB specific TABLESAMPLE clause"""
        if not self.dsl.population.sampling or not self.dsl.population.sampling.enabled:
            return ""

        sampling = self.dsl.population.sampling
        seed_clause = (
            f" REPEATABLE ({sampling.random_seed})" if sampling.random_seed else ""
        )

        if sampling.sample_size:
            return f" TABLESAMPLE RESERVOIR({sampling.sample_size} ROWS){seed_clause}"
        elif sampling.sample_percentage:
            pct = int(sampling.sample_percentage * 100)
            return f" TABLESAMPLE RESERVOIR({pct}%){seed_clause}"

        return ""

    def _construct_query(
        self, final_population_alias: str, sampling_clause: str
    ) -> str:
        """Assembles final SQL query"""
        # Build CTE chain
        cte_sql = "WITH " + ",\n".join(self.cte_fragments) if self.cte_fragments else ""

        # 1. Assemble Population Filters (AND)
        pop_clause = (
            " AND ".join(self.population_filters) if self.population_filters else "1=1"
        )

        # 2. Assemble Exceptions (OR)
        if self.assertion_exceptions:
            exceptions_clause = " OR ".join(self.assertion_exceptions)
            # Final WHERE: Must be in population, AND must break AT LEAST ONE rule
            where_clause = f"({pop_clause}) \n  AND ({exceptions_clause})"
        else:
            where_clause = pop_clause

        # Build SELECT
        if self.having_conditions:
            # Aggregation query
            group_fields = ", ".join(set(self.group_by_fields))
            having_clause = " AND ".join(self.having_conditions)

            # Find the metric field from assertions
            metric_field = None
            for assertion in self.dsl.assertions:
                if isinstance(assertion, AggregationSumAssertion):
                    metric_field = self._normalize_field_name(assertion.metric_field)
                    break

            select_sql = f"""
SELECT {group_fields}, 
       COUNT(*) as exception_count,
       SUM({metric_field}) as total_amount
FROM {final_population_alias}
WHERE {where_clause}
GROUP BY {group_fields}
HAVING {having_clause}"""
        else:
            # Row-level query
            select_sql = f"""
SELECT *
FROM {final_population_alias}{sampling_clause}
WHERE {where_clause}"""

        if cte_sql:
            return f"{cte_sql}\n{select_sql}"
        else:
            return select_sql

    @staticmethod
    def _quote_value(value: Any) -> str:
        """
        Safely quotes SQL values with proper escaping.
        CRITICAL FIX: Escape single quotes to prevent SQL injection
        """
        if isinstance(value, str):
            # Escape single quotes (O'Connor -> O''Connor)
            safe_val = value.replace("'", "''")
            return f"'{safe_val}'"
        elif isinstance(value, datetime):
            return f"'{value.isoformat()}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif value is None:
            return "NULL"
        return str(value)
