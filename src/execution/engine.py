"""
Execution Engine Module
DuckDB-based execution with disk streaming
"""

from datetime import datetime
from typing import Any, Dict

import duckdb

from src.compiler.sql_compiler import ControlCompiler
from src.models.dsl import EnterpriseControlDSL
from src.utils.logging_config import get_logger

# Get logger for this module
logger = get_logger(__name__)


class ExecutionEngine:
    """
    Executes compiled SQL against Parquet files using DuckDB.
    Uses disk-streaming to avoid memory bloat on large datasets.
    """

    def __init__(self, db_path: str = ":memory:"):
        logger.info(f"Initializing ExecutionEngine with db_path={db_path}")
        self.conn = duckdb.connect(db_path)
        # Enable Parquet extensions
        logger.debug("Installing and loading Parquet extension")
        self.conn.execute("INSTALL parquet")
        self.conn.execute("LOAD parquet")
        logger.info("ExecutionEngine initialized successfully")

    def execute_control(
        self, dsl: EnterpriseControlDSL, manifests: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Executes control and returns exception report.

        Args:
            dsl: The validated control DSL
            manifests: Output from EvidenceIngestion.ingest_excel_to_parquet()

        Returns:
            Execution report with verdict, exceptions, and audit metadata
        """
        logger.info(f"Executing control: {dsl.governance.control_id}")
        logger.debug(f"Manifests: {list(manifests.keys())}")

        # Compile DSL to SQL
        logger.debug("Compiling DSL to SQL")
        compiler = ControlCompiler(dsl)
        sql = compiler.compile_to_sql(manifests)
        logger.debug(f"SQL compilation complete, query length: {len(sql)} chars")

        try:
            # Execute query (DuckDB streams from disk - no RAM bloat)
            logger.debug("Executing SQL query via DuckDB")
            result = self.conn.execute(sql).df()
            exception_count = len(result)
            logger.info(
                f"Query executed successfully, {exception_count} exceptions found"
            )

            # Calculate population size
            logger.debug("Calculating population count")
            total_population = self._get_population_count(manifests, dsl, compiler)
            logger.info(f"Total population: {total_population}")

            # CRITICAL SAFEGUARD: Detect empty data feeds
            if total_population == 0:
                logger.error(
                    f"Zero population detected for control {dsl.governance.control_id}"
                )
                return {
                    "control_id": dsl.governance.control_id,
                    "verdict": "ERROR",
                    "error_message": "Zero Population: The base dataset contains 0 rows after filters. Cannot attest to control effectiveness. Possible upstream data feed failure.",
                    "exception_count": 0,
                    "total_population": 0,
                    "execution_query": sql,
                    "evidence_hashes": {
                        alias: meta["sha256_hash"] for alias, meta in manifests.items()
                    },
                    "executed_at": datetime.now().isoformat(),
                }

            # Calculate exception rate
            exception_rate = (
                (exception_count / total_population * 100)
                if total_population > 0
                else 0
            )

            # Determine verdict based on materiality threshold
            max_threshold = max(
                [a.materiality_threshold_percent for a in dsl.assertions], default=0.0
            )
            verdict = "PASS" if exception_rate <= max_threshold else "FAIL"

            logger.info(
                f"Control {dsl.governance.control_id} executed: verdict={verdict}, "
                f"exceptions={exception_count}/{total_population} ({exception_rate:.2f}%), "
                f"threshold={max_threshold}%"
            )

            return {
                "control_id": dsl.governance.control_id,
                "verdict": verdict,
                "exception_count": exception_count,
                "total_population": total_population,
                "exception_rate_percent": round(exception_rate, 2),
                "materiality_threshold_percent": max_threshold,
                "execution_query": sql,
                "evidence_hashes": {
                    alias: meta["sha256_hash"] for alias, meta in manifests.items()
                },
                "exceptions_sample": result.head(100).to_dict(orient="records"),
                "executed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(
                f"Execution error for control {dsl.governance.control_id}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            logger.debug(f"Failed SQL query: {sql}")
            return {
                "control_id": dsl.governance.control_id,
                "verdict": "ERROR",
                "error_message": str(e),
                "error_type": type(e).__name__,
                "execution_query": sql,
                "executed_at": datetime.now().isoformat(),
            }

    def _get_population_count(
        self,
        manifests: Dict[str, Dict[str, Any]],
        dsl: EnterpriseControlDSL,
        compiler: ControlCompiler,
    ) -> int:
        """
        Counts total rows in the population after filters and joins but before assertions.

        CRITICAL: Must use the same CTE chain as the main query since population_filters
        may reference columns from joined datasets that don't exist in base dataset.

        CRITICAL: Creates a fresh compiler instance to avoid state corruption from
        reusing the same compiler that will be used for main query execution.
        """
        logger.debug(f"Getting population count for {dsl.governance.control_id}")

        try:
            # CRITICAL FIX: Create a fresh compiler instance to avoid CTE name collisions
            # The compiler passed in will be used for the main query, so we can't modify its state
            from src.compiler.sql_compiler import ControlCompiler

            count_compiler = ControlCompiler(dsl)

            # Build the CTE chain with this fresh compiler
            final_cte_alias = count_compiler._build_population_cte(manifests)

            # Build WHERE clause from population filters
            if count_compiler.population_filters:
                where_clause = " AND ".join(count_compiler.population_filters)
                count_sql = f"""
WITH {", ".join(count_compiler.cte_fragments)}
SELECT COUNT(*) FROM {final_cte_alias}
WHERE {where_clause}
"""
            else:
                count_sql = f"""
WITH {", ".join(count_compiler.cte_fragments)}
SELECT COUNT(*) FROM {final_cte_alias}
"""

            logger.debug(f"Population count SQL: {count_sql}")
            result = self.conn.execute(count_sql).fetchone()
            count = result[0] if result is not None else 0
            logger.debug(f"Population count: {count}")
            return count

        except Exception as e:
            # Log the error with full traceback for debugging
            logger.error(
                f"Failed to get population count: {type(e).__name__}: {e}",
                exc_info=True,
            )
            logger.debug(
                f"Attempted SQL: {count_sql if 'count_sql' in locals() else 'Not generated'}"
            )

            # Fallback to base manifest count (may be inaccurate if filters/joins exist)
            base_alias = dsl.population.base_dataset
            fallback_count = manifests[base_alias].get("row_count", 0)
            logger.warning(
                f"Using fallback manifest count ({fallback_count}) - "
                f"this may be inaccurate if joins or filters are present"
            )
            return fallback_count

    def validate_sql_dry_run(self, sql: str) -> tuple[bool, str]:
        """
        Deterministic SQL validation using DuckDB's EXPLAIN mechanism.
        Forces DuckDB to parse and bind the query to Parquet schemas without executing.

        This is the STRICT JUDGE that enforces correctness.
        Returns False and the exact error message if SQL is invalid.

        Args:
            sql: The compiled SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)
            - (True, "Valid") if SQL is correct
            - (False, error_message) if SQL has issues
        """
        logger.debug("Running SQL dry-run validation via EXPLAIN")
        try:
            # EXPLAIN triggers Parser (syntax) and Binder (schema) validation
            # without executing the query over data
            self.conn.execute(f"EXPLAIN {sql}")
            logger.info("SQL dry-run validation PASSED")
            return True, "Valid"

        except duckdb.BinderException as e:
            # Semantic error (e.g., column doesn't exist, ambiguous join)
            error_msg = f"Binder Error: {str(e)}"
            logger.warning(f"SQL validation failed: {error_msg}")
            return False, error_msg

        except duckdb.ParserException as e:
            # Syntax error (e.g., missing parenthesis, invalid SQL)
            error_msg = f"Parser Error: {str(e)}"
            logger.warning(f"SQL validation failed: {error_msg}")
            return False, error_msg

        except Exception as e:
            # Catch-all for other DuckDB errors
            error_msg = f"DuckDB Validation Error: {type(e).__name__}: {str(e)}"
            logger.warning(f"SQL validation failed: {error_msg}")
            return False, error_msg

    def validate_schema(
        self, manifests: Dict[str, Dict[str, Any]], dsl: EnterpriseControlDSL
    ) -> Dict[str, Any]:
        """
        Pre-flight schema validation: check if expected columns exist in Parquet.
        Returns validation result with missing columns if any.
        """
        logger.debug(f"Validating schema for control {dsl.governance.control_id}")
        expected_columns = {
            binding.technical_field for binding in dsl.ontology_bindings
        }
        logger.debug(f"Expected columns: {expected_columns}")

        validation_results = []

        for dataset_alias, manifest in manifests.items():
            actual_columns = set(manifest.get("columns", []))
            missing = expected_columns - actual_columns

            if missing:
                logger.warning(
                    f"Schema drift detected in {dataset_alias}: missing columns {missing}"
                )
                validation_results.append(
                    {
                        "dataset_alias": dataset_alias,
                        "status": "SCHEMA_DRIFT_DETECTED",
                        "missing_columns": list(missing),
                        "available_columns": list(actual_columns),
                    }
                )
            else:
                validation_results.append(
                    {"dataset_alias": dataset_alias, "status": "VALID"}
                )

        overall_status = (
            "VALID"
            if all(r["status"] == "VALID" for r in validation_results)
            else "SCHEMA_DRIFT_DETECTED"
        )

        logger.info(f"Schema validation complete: {overall_status}")

        return {
            "overall_status": overall_status,
            "dataset_validations": validation_results,
        }

    def close(self):
        """Close DuckDB connection"""
        logger.info("Closing DuckDB connection")
        self.conn.close()
        logger.debug("DuckDB connection closed")
