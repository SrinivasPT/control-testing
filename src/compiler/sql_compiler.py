"""
SQL Compiler Module
Translates DSL to DuckDB SQL with proper CTE chaining and value escaping
"""

from typing import List, Dict, Any
from datetime import datetime
from src.models.dsl import (
    EnterpriseControlDSL,
    ValueMatchAssertion,
    TemporalSequenceAssertion,
    AggregationSumAssertion,
    FilterComparison,
    FilterInList,
    JoinLeft,
)


class ControlCompiler:
    """Compiles DSL into DuckDB SQL with exception detection logic"""

    def __init__(self, dsl: EnterpriseControlDSL):
        self.dsl = dsl
        self.where_conditions: List[str] = []
        self.having_conditions: List[str] = []
        self.group_by_fields: List[str] = []
        self.cte_fragments: List[str] = []

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
                self.where_conditions.append(cond)

            elif action.operation == "filter_in_list":
                cond = self._compile_filter_in_list(action)
                self.where_conditions.append(cond)

            elif action.operation == "join_left":
                right_path = manifests[action.right_dataset]["parquet_path"]
                # CRITICAL FIX: Reference previous step, not always 'base'
                join_cte = f"""{step.step_id} AS (
    SELECT * FROM {previous_alias}
    LEFT JOIN read_parquet('{right_path}') AS right_tbl
    ON {previous_alias}.{action.left_key} = right_tbl.{action.right_key}
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
        return f"{action.field} {sql_op} {self._quote_value(action.value)}"

    def _compile_filter_in_list(self, action: FilterInList) -> str:
        """Compiles FilterInList to SQL IN condition"""
        values_str = ", ".join([self._quote_value(v) for v in action.values])
        return f"{action.field} IN ({values_str})"

    def _compile_assertions(self) -> None:
        """Routes assertions to appropriate SQL clauses"""
        for assertion in self.dsl.assertions:
            if isinstance(assertion, ValueMatchAssertion):
                # Row-level assertion → WHERE clause
                cond = self._compile_value_match(assertion)
                # Wrap in NOT to find exceptions
                self.where_conditions.append(f"NOT ({cond})")

            elif isinstance(assertion, TemporalSequenceAssertion):
                # Temporal sequence → WHERE clause
                cond = self._compile_temporal_sequence(assertion)
                self.where_conditions.append(f"NOT ({cond})")

            elif isinstance(assertion, AggregationSumAssertion):
                # Aggregation assertion → HAVING clause
                cond = self._compile_aggregation(assertion)
                self.having_conditions.append(f"NOT ({cond})")
                self.group_by_fields.extend(assertion.group_by_fields)

    def _compile_value_match(self, assertion: ValueMatchAssertion) -> str:
        """Translates ValueMatchAssertion to SQL condition"""
        field = assertion.field
        operator = assertion.operator
        value = assertion.expected_value

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

        # Handle scalar values
        return f"{field} {sql_op} {self._quote_value(value)}"

    def _compile_temporal_sequence(self, assertion: TemporalSequenceAssertion) -> str:
        """Translates TemporalSequenceAssertion to SQL condition"""
        # Build chain: event1 < event2 < event3 ...
        conditions = []
        for i in range(len(assertion.event_chain) - 1):
            event1 = assertion.event_chain[i]
            event2 = assertion.event_chain[i + 1]
            conditions.append(f"{event1} < {event2}")

        return " AND ".join(conditions)

    def _compile_aggregation(self, assertion: AggregationSumAssertion) -> str:
        """Translates AggregationSumAssertion to SQL HAVING condition"""
        metric = assertion.metric_field
        operator = assertion.operator
        threshold = assertion.threshold

        op_map = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}
        sql_op = op_map[operator]

        return f"SUM({metric}) {sql_op} {threshold}"

    def _build_sampling_clause(self) -> str:
        """Builds sampling clause if enabled"""
        if not self.dsl.population.sampling or not self.dsl.population.sampling.enabled:
            return ""

        sampling = self.dsl.population.sampling

        if sampling.sample_size:
            # Absolute sample size
            return f"\nUSING SAMPLE {sampling.sample_size} ROWS"
        elif sampling.sample_percentage:
            # Percentage-based sampling
            pct = int(sampling.sample_percentage * 100)
            return f"\nUSING SAMPLE {pct}%"

        return ""

    def _construct_query(
        self, final_population_alias: str, sampling_clause: str
    ) -> str:
        """Assembles final SQL query"""
        # Build CTE chain
        cte_sql = "WITH " + ",\n".join(self.cte_fragments) if self.cte_fragments else ""

        # Build WHERE clause
        where_clause = (
            " AND ".join(self.where_conditions) if self.where_conditions else "1=1"
        )

        # Build SELECT
        if self.having_conditions:
            # Aggregation query
            group_fields = ", ".join(set(self.group_by_fields))
            having_clause = " AND ".join(self.having_conditions)

            # Find the metric field from assertions
            metric_field = None
            for assertion in self.dsl.assertions:
                if isinstance(assertion, AggregationSumAssertion):
                    metric_field = assertion.metric_field
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
