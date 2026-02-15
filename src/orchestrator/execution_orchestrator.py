"""
Execution Orchestrator Module
Single Responsibility: Coordinate SQL compilation, validation, and execution
"""

from typing import Any, Dict, Tuple

from src.compiler.sql_compiler import ControlCompiler
from src.execution.engine import ExecutionEngine
from src.models.dsl import EnterpriseControlDSL
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class ExecutionOrchestrator:
    """
    Coordinates SQL compilation, validation, and execution.
    Separates execution concerns from orchestration logic.
    """

    def __init__(self, engine: ExecutionEngine):
        """
        Initialize execution orchestrator.

        Args:
            engine: Execution engine for SQL validation and execution
        """
        self.engine = engine
        logger.debug("ExecutionOrchestrator initialized")

    def compile_and_validate(
        self, dsl: EnterpriseControlDSL, manifests: Dict[str, Dict[str, Any]]
    ) -> Tuple[str, bool, str]:
        """
        Compiles DSL to SQL and validates it with DuckDB EXPLAIN.

        Args:
            dsl: Control DSL
            manifests: Parquet manifests

        Returns:
            Tuple of (sql, is_valid, error_msg)
        """
        logger.debug(f"Compiling DSL to SQL for {dsl.governance.control_id}")

        # Compile DSL to SQL
        compiler = ControlCompiler(dsl)
        sql = compiler.compile_to_sql(manifests)

        logger.debug(f"SQL compiled, length: {len(sql)} characters")

        # Validate with DuckDB
        is_valid, error_msg = self.engine.validate_sql_dry_run(sql)

        if is_valid:
            logger.info(f"SQL validation PASSED for {dsl.governance.control_id}")
        else:
            logger.warning(
                f"SQL validation FAILED for {dsl.governance.control_id}: {error_msg[:100]}"
            )

        return sql, is_valid, error_msg

    def execute(
        self, dsl: EnterpriseControlDSL, manifests: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Executes control test via DuckDB.

        Args:
            dsl: Control DSL
            manifests: Parquet manifests

        Returns:
            Execution report with verdict and exception details
        """
        logger.info(f"Executing control test for {dsl.governance.control_id}")

        report = self.engine.execute_control(dsl, manifests)

        logger.info(
            f"Execution complete: {dsl.governance.control_id}, verdict={report['verdict']}"
        )

        return report
