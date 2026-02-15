"""
Validation Orchestrator Module
Single Responsibility: Coordinate LLM validation flow
"""

from typing import Any, Dict, List, Optional

from src.ai.validator import AIValidator
from src.models.dsl import EnterpriseControlDSL
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class ValidationResult:
    """Data class for validation results"""

    def __init__(
        self,
        is_valid: bool,
        critical_issues: List,
        dsl_report: Optional[Any] = None,
        sql_report: Optional[Any] = None,
    ):
        self.is_valid = is_valid
        self.critical_issues = critical_issues
        self.dsl_report = dsl_report
        self.sql_report = sql_report


class ValidationOrchestrator:
    """
    Coordinates LLM-based validation of DSL and SQL.
    Optional layer - can be disabled for performance.
    """

    def __init__(self, validator: Optional[AIValidator]):
        """
        Initialize validation orchestrator.

        Args:
            validator: AI validator instance (None to disable validation)
        """
        self.validator = validator
        self.enabled = validator is not None
        logger.debug(f"ValidationOrchestrator initialized (enabled={self.enabled})")

    def validate(
        self,
        control_text: str,
        dsl: EnterpriseControlDSL,
        sql: str,
        manifests: Dict[str, Dict[str, Any]],
    ) -> Optional[ValidationResult]:
        """
        Performs LLM validation of DSL and SQL.

        Args:
            control_text: Original control procedure text
            dsl: Generated DSL
            sql: Compiled SQL
            manifests: Parquet manifests

        Returns:
            ValidationResult or None if validation is disabled
        """
        if not self.enabled or self.validator is None:
            logger.debug("Validation skipped (validator not enabled)")
            return None

        logger.info("Starting LLM validation")

        try:
            validation_results = self.validator.validate_full_pipeline(
                control_text, dsl, sql, manifests
            )

            dsl_report = validation_results["dsl_validation"]
            sql_report = validation_results["sql_validation"]

            # Extract critical issues
            critical_issues = [
                issue
                for issue in dsl_report.issues + sql_report.issues
                if issue.severity == "CRITICAL"
            ]

            is_valid = dsl_report.is_valid and sql_report.is_valid

            logger.info(
                f"LLM Validation: DSL={dsl_report.is_valid}, SQL={sql_report.is_valid}, "
                f"Critical Issues={len(critical_issues)}"
            )

            return ValidationResult(
                is_valid=is_valid,
                critical_issues=critical_issues,
                dsl_report=dsl_report,
                sql_report=sql_report,
            )

        except Exception as e:
            logger.error(
                f"LLM validation failed: {type(e).__name__}: {e}", exc_info=True
            )
            # Non-fatal - continue to DuckDB validation
            return None
