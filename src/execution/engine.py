"""
Execution Engine Module
DuckDB-based execution with disk streaming
"""

from datetime import datetime
from typing import Any, Dict

import duckdb

from src.compiler.sql_compiler import ControlCompiler
from src.models.dsl import EnterpriseControlDSL


class ExecutionEngine:
    """
    Executes compiled SQL against Parquet files using DuckDB.
    Uses disk-streaming to avoid memory bloat on large datasets.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        # Enable Parquet extensions
        self.conn.execute("INSTALL parquet")
        self.conn.execute("LOAD parquet")

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
        # Compile DSL to SQL
        compiler = ControlCompiler(dsl)
        sql = compiler.compile_to_sql(manifests)

        try:
            # Execute query (DuckDB streams from disk - no RAM bloat)
            result = self.conn.execute(sql).df()
            exception_count = len(result)

            # Calculate population size
            total_population = self._get_population_count(manifests, dsl, compiler)

            # CRITICAL SAFEGUARD: Detect empty data feeds
            if total_population == 0:
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
        Counts total rows in the population after filters but before assertions.
        """
        base_alias = dsl.population.base_dataset
        base_path = manifests[base_alias]["parquet_path"]

        # Use the strictly segregated population_filters from the updated compiler
        if hasattr(compiler, "population_filters") and compiler.population_filters:
            where_clause = " AND ".join(compiler.population_filters)
            count_sql = (
                f"SELECT COUNT(*) FROM read_parquet('{base_path}') WHERE {where_clause}"
            )
        else:
            count_sql = f"SELECT COUNT(*) FROM read_parquet('{base_path}')"

        try:
            result = self.conn.execute(count_sql).fetchone()
            return result[0] if result is not None else 0
        except Exception:
            # Log the error in production, fallback to manifest count
            return manifests[base_alias].get("row_count", 0)

    def validate_schema(
        self, manifests: Dict[str, Dict[str, Any]], dsl: EnterpriseControlDSL
    ) -> Dict[str, Any]:
        """
        Pre-flight schema validation: check if expected columns exist in Parquet.
        Returns validation result with missing columns if any.
        """
        expected_columns = {
            binding.technical_field for binding in dsl.ontology_bindings
        }

        validation_results = []

        for dataset_alias, manifest in manifests.items():
            actual_columns = set(manifest.get("columns", []))
            missing = expected_columns - actual_columns

            if missing:
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

        return {
            "overall_status": overall_status,
            "dataset_validations": validation_results,
        }

    def close(self):
        """Close DuckDB connection"""
        self.conn.close()
