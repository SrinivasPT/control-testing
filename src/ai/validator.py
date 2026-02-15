"""
AI Validation Module
LLM-powered validation of DSL and SQL before execution.
Acts as a "second pair of eyes" to catch semantic errors.
"""

import json
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from src.models.dsl import EnterpriseControlDSL
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class ValidationIssue(BaseModel):
    """Represents a validation issue found by the LLM reviewer"""

    severity: str = Field(description="Severity level: CRITICAL, WARNING, INFO")
    category: str = Field(
        description="Category: DSL_STRUCTURE, SCHEMA_MISMATCH, SQL_LOGIC, SEMANTICS"
    )
    message: str = Field(description="Human-readable description of the issue")
    suggested_fix: Optional[str] = Field(
        default=None, description="If available, suggested fix for the issue"
    )


class ValidationReport(BaseModel):
    """Structured validation report from LLM reviewer"""

    is_valid: bool = Field(description="True if no CRITICAL issues found")
    issues: List[ValidationIssue] = Field(
        default_factory=list, description="List of issues found during validation"
    )
    overall_assessment: str = Field(
        description="High-level summary of validation results"
    )
    confidence_score: float = Field(
        ge=0.0, le=1.0, description="Confidence in the validation (0.0-1.0)"
    )


DSL_VALIDATION_PROMPT = """You are a Principal Banking Compliance Auditor and Data Architect reviewing a generated control DSL.

CONTEXT:
Your role is to validate that the generated DSL correctly implements the control requirements, 
uses proper schema bindings, and will execute correctly against the available data.

INPUTS:
1. CONTROL REQUIREMENTS (original business requirement):
{control_text}

2. GENERATED DSL (JSON):
{dsl_json}

3. AVAILABLE SCHEMAS (actual Parquet columns):
{schema_info}

VALIDATION CHECKLIST:
□ Does the DSL accurately implement the control requirements?
□ Are all ontology_bindings mapped to actual columns in the schemas?
□ Are population filters correctly scoped (not filtering out records that should fail)?
□ Are assertions checking the RIGHT conditions (not inverted logic)?
□ For LEFT JOINs, are NULL checks correctly placed?
  - If "absence of join = compliance", filter WHERE right_key IS NOT NULL in population
  - If "absence of join = failure", assert right_key IS NOT NULL in assertions
□ Are column names qualified correctly for joined datasets?
□ For composite joins, are left_keys and right_keys both LISTS?
□ Are operators valid for the assertion type?
□ Are materiality thresholds reasonable?
□ Is the evidence metadata complete?

COMMON ERRORS TO CATCH:
1. **Filter vs Assertion Trap**: Missing records filtered in population instead of caught as failures in assertions
2. **NULL Semantics**: Not handling LEFT JOIN NULLs correctly
3. **Schema Drift**: Referencing columns that don't exist in the Parquet files
4. **Inverted Logic**: Checking "status = APPROVED" when control says "must NOT be approved"
5. **Unqualified Joins**: Using "tax_id" instead of "ofac_list.tax_id" after a join
6. **Semantic Mismatch**: DSL implements something different than the control requirement

OUTPUT (JSON):
{{
  "is_valid": true/false,
  "issues": [
    {{
      "severity": "CRITICAL|WARNING|INFO",
      "category": "DSL_STRUCTURE|SCHEMA_MISMATCH|SQL_LOGIC|SEMANTICS",
      "message": "Clear description of the issue",
      "suggested_fix": "How to fix it (optional)"
    }}
  ],
  "overall_assessment": "Brief summary",
  "confidence_score": 0.0-1.0
}}

SEVERITY LEVELS:
- CRITICAL: Will cause execution failure or produce wrong results
- WARNING: Suboptimal but may work
- INFO: Best practice suggestion

Be thorough but practical. Focus on correctness, not style.
"""


SQL_VALIDATION_PROMPT = """You are a DuckDB SQL Expert reviewing compiled SQL for correctness.

CONTEXT:
A DSL was compiled into DuckDB SQL. Your job is to validate:
1. SQL syntax correctness
2. Logical correctness (does it match the control intent?)
3. Performance/anti-patterns

INPUTS:
1. CONTROL REQUIREMENTS:
{control_text}

2. GENERATED DSL:
{dsl_summary}

3. COMPILED SQL:
{sql_query}

4. AVAILABLE SCHEMAS:
{schema_info}

VALIDATION CHECKLIST:
□ Is the SQL syntactically valid?
□ Are all referenced columns present in the FROM/JOIN clauses?
□ Are CTEs properly chained (each CTE references previous one)?
□ Are NULL semantics handled correctly in WHERE/HAVING?
□ Are string values properly escaped?
□ Are date comparisons using correct syntax?
□ Does the WHERE clause correctly separate population filters from assertion exceptions?
□ Are aggregations (SUM, COUNT, AVG) in HAVING clause, not WHERE?
□ Does the final SELECT return EXCEPTIONS (failing rows), not compliant rows?

COMMON SQL ERRORS:
1. Referencing column from wrong CTE (e.g., using base.col_name after a join)
2. Ambiguous column names in joins
3. Missing CAST for date/number comparisons
4. NOT (NULL) = NULL instead of IS NOT TRUE
5. Using AND when OR is needed (or vice versa)
6. Returning compliant rows instead of exceptions

OUTPUT (JSON):
{{
  "is_valid": true/false,
  "issues": [/* same format as DSL validation */],
  "overall_assessment": "Brief summary",
  "confidence_score": 0.0-1.0
}}

Be strict. SQL errors at runtime are expensive.
"""


class AIValidator:
    """
    LLM-powered validator that reviews DSL and SQL before execution.
    Uses structured output to ensure deterministic validation reporting.
    """

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-chat"):
        self.model = model
        logger.info(f"Initializing AIValidator with model: {model}")

        try:
            import instructor
            from openai import OpenAI
        except ImportError:
            logger.error("Instructor library not installed")
            raise ImportError(
                "Instructor library not installed. "
                "Run: pip install instructor openai pydantic"
            )

        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            logger.error("DeepSeek API key not provided")
            raise ValueError(
                "DeepSeek API key required. Set DEEPSEEK_API_KEY environment variable"
            )

        base_client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        self.client = instructor.from_openai(base_client)
        logger.info("AIValidator initialized successfully")

    def validate_dsl(
        self,
        control_text: str,
        dsl: EnterpriseControlDSL,
        manifests: Dict[str, Dict[str, Any]],
    ) -> ValidationReport:
        """
        Validates the generated DSL against control requirements and schemas.

        Args:
            control_text: Original control requirement text
            dsl: Generated DSL object
            manifests: Parquet manifests with schema information

        Returns:
            ValidationReport with issues and recommendations
        """
        logger.info(f"Validating DSL for control {dsl.governance.control_id}")

        # Prepare schema information
        schema_info = self._format_schema_info(manifests)

        # Convert DSL to JSON for LLM review
        dsl_json = json.dumps(dsl.model_dump(), indent=2)

        prompt = DSL_VALIDATION_PROMPT.format(
            control_text=control_text, dsl_json=dsl_json, schema_info=schema_info
        )

        try:
            logger.debug("Calling LLM for DSL validation")
            report = self.client.chat.completions.create(
                model=self.model,
                response_model=ValidationReport,
                max_retries=3,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert banking compliance auditor.",
                    },
                    {"role": "user", "content": prompt},
                ],
            )

            logger.info(
                f"DSL validation complete: "
                f"is_valid={report.is_valid}, "
                f"issues={len(report.issues)}, "
                f"confidence={report.confidence_score:.2f}"
            )

            # Log critical issues
            for issue in report.issues:
                if issue.severity == "CRITICAL":
                    logger.error(f"CRITICAL DSL Issue: {issue.message}")
                elif issue.severity == "WARNING":
                    logger.warning(f"DSL Warning: {issue.message}")

            return report

        except Exception as e:
            logger.error(
                f"DSL validation failed: {type(e).__name__}: {e}", exc_info=True
            )
            # Return error report
            return ValidationReport(
                is_valid=False,
                issues=[
                    ValidationIssue(
                        severity="CRITICAL",
                        category="VALIDATION_ERROR",
                        message=f"Validation system error: {str(e)}",
                        suggested_fix="Check validator configuration and API availability",
                    )
                ],
                overall_assessment="Validation failed due to system error",
                confidence_score=0.0,
            )

    def validate_sql(
        self,
        control_text: str,
        dsl: EnterpriseControlDSL,
        sql: str,
        manifests: Dict[str, Dict[str, Any]],
    ) -> ValidationReport:
        """
        Validates the compiled SQL against DSL and control requirements.

        Args:
            control_text: Original control requirement text
            dsl: DSL object that was compiled
            sql: Compiled SQL query
            manifests: Parquet manifests with schema information

        Returns:
            ValidationReport with SQL-specific issues
        """
        logger.info(f"Validating SQL for control {dsl.governance.control_id}")

        # Prepare schema information
        schema_info = self._format_schema_info(manifests)

        # Create DSL summary for context
        dsl_summary = self._create_dsl_summary(dsl)

        prompt = SQL_VALIDATION_PROMPT.format(
            control_text=control_text,
            dsl_summary=dsl_summary,
            sql_query=sql,
            schema_info=schema_info,
        )

        try:
            logger.debug("Calling LLM for SQL validation")
            report = self.client.chat.completions.create(
                model=self.model,
                response_model=ValidationReport,
                max_retries=3,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert DuckDB SQL database engineer.",
                    },
                    {"role": "user", "content": prompt},
                ],
            )

            logger.info(
                f"SQL validation complete: "
                f"is_valid={report.is_valid}, "
                f"issues={len(report.issues)}, "
                f"confidence={report.confidence_score:.2f}"
            )

            # Log critical issues
            for issue in report.issues:
                if issue.severity == "CRITICAL":
                    logger.error(f"CRITICAL SQL Issue: {issue.message}")
                elif issue.severity == "WARNING":
                    logger.warning(f"SQL Warning: {issue.message}")

            return report

        except Exception as e:
            logger.error(
                f"SQL validation failed: {type(e).__name__}: {e}", exc_info=True
            )
            return ValidationReport(
                is_valid=False,
                issues=[
                    ValidationIssue(
                        severity="CRITICAL",
                        category="VALIDATION_ERROR",
                        message=f"SQL validation system error: {str(e)}",
                        suggested_fix="Check validator configuration and API availability",
                    )
                ],
                overall_assessment="SQL validation failed due to system error",
                confidence_score=0.0,
            )

    def validate_full_pipeline(
        self,
        control_text: str,
        dsl: EnterpriseControlDSL,
        sql: str,
        manifests: Dict[str, Dict[str, Any]],
    ) -> Dict[str, ValidationReport]:
        """
        Performs complete validation: DSL + SQL.

        Args:
            control_text: Original control requirement
            dsl: Generated DSL
            sql: Compiled SQL
            manifests: Parquet manifests

        Returns:
            Dict with 'dsl_validation' and 'sql_validation' reports
        """
        logger.info(f"Running full pipeline validation for {dsl.governance.control_id}")

        dsl_report = self.validate_dsl(control_text, dsl, manifests)
        sql_report = self.validate_sql(control_text, dsl, sql, manifests)

        # Summary logging
        overall_valid = dsl_report.is_valid and sql_report.is_valid
        total_critical = sum(
            1
            for issue in dsl_report.issues + sql_report.issues
            if issue.severity == "CRITICAL"
        )

        logger.info(
            f"Full pipeline validation: "
            f"overall_valid={overall_valid}, "
            f"critical_issues={total_critical}"
        )

        return {
            "dsl_validation": dsl_report,
            "sql_validation": sql_report,
            "overall_valid": overall_valid,
            "total_critical_issues": total_critical,
        }

    def _format_schema_info(self, manifests: Dict[str, Dict[str, Any]]) -> str:
        """Formats manifest schema info for LLM consumption"""
        schema_lines = []
        for alias, meta in manifests.items():
            cols = meta.get("columns", [])
            schema_lines.append(f"Dataset: {alias}")
            schema_lines.append(f"  Columns: {', '.join(cols)}")
            schema_lines.append(f"  Rows: {meta.get('row_count', 'unknown')}")
            schema_lines.append("")
        return "\n".join(schema_lines)

    def _create_dsl_summary(self, dsl: EnterpriseControlDSL) -> str:
        """Creates a concise DSL summary for SQL validation context"""
        summary = [
            f"Control ID: {dsl.governance.control_id}",
            f"Population Base: {dsl.population.base_dataset}",
            f"Pipeline Steps: {len(dsl.population.steps)}",
            f"Assertions: {len(dsl.assertions)}",
            "",
            "Assertion Types:",
        ]

        for i, assertion in enumerate(dsl.assertions, 1):
            summary.append(f"  {i}. {assertion.assertion_type}")

        return "\n".join(summary)
