"""
AI Translation Module
Schema pruning and DSL generation using OpenAI/Anthropic
"""

import json
import os
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError

from src.models.dsl import EnterpriseControlDSL
from src.utils.logging_config import get_logger

# Get logger for this module
logger = get_logger(__name__)


# NEW: Define a strict Pydantic model for the Pruning Pass
class PrunedSchema(BaseModel):
    """Structured output for the schema pruning LLM pass"""

    required_columns: List[str] = Field(
        description="List of required columns in format 'dataset_alias.column_name'"
    )
    reasoning: str = Field(
        description="Brief explanation of why these columns were selected"
    )


# System Prompts
PRUNING_PROMPT = """You are a banking data architect.

TASK: Analyze the control procedure and identify the MINIMUM required columns.

INPUT:
- Control Procedure: {control_text}
- Available Columns: {all_column_names}

OUTPUT (JSON):
{{
  "required_columns": ["col1", "col2", "col3"],
  "reasoning": "Brief explanation of why each column is needed"
}}

RULES:
- Return 3-7 columns maximum
- Use EXACT column names from the provided list
- Prefer explicit columns over wildcards
"""

DSL_GENERATION_PROMPT = """You are a strict compliance control compiler.

TASK: Translate the control procedure into EnterpriseControlDSL JSON.

INPUT:
- Control Procedure: {control_text}
- Pruned Schema: {selected_columns_with_types}
- Evidence Metadata: {dataset_aliases}

OUTPUT: Return a JSON object with these EXACT top-level keys:
- governance
- ontology_bindings
- population
- assertions
- evidence

DO NOT wrap the JSON in any additional keys like "control" or "dsl". The top-level keys MUST be the five listed above.

CRITICAL RULES:
1. Use ONLY the columns from the pruned schema
2. Do NOT invent operators - use only from allowed enums
3. Map business terms to technical fields using ontology_bindings
4. For row-level checks, use ValueMatchAssertion with assertion_type="value_match"
5. For aggregations (SUM, AVG, COUNT, MIN, MAX), use AggregationAssertion with assertion_type="aggregation"
6. Include materiality thresholds (typically 0.0 for binary checks)
7. DO NOT wrap the result in a "control" key - return the fields directly at the top level

8. **CRITICAL - Qualified Column Names in Joins:**
   - When referencing a column from a JOINED dataset in an assertion, you MUST prefix it with the dataset alias
   - Example: Use "ofac_watch_list_sheet1.tax_id" instead of just "tax_id" after joining
   - For columns from the base dataset, you can use the unqualified name

9. **CRITICAL - Composite Joins:**
   - JoinLeft now accepts "left_keys" and "right_keys" as LISTS, not single strings
   - To join on multiple columns, use: "left_keys": ["employee_id", "ticker_symbol"]
   - NEVER use a filter to complete a join - use composite keys in the join itself

10. **CRITICAL - NULL Handling in Left Joins:**
    - When doing a LEFT JOIN to check restrictions, if the absence of a record means compliance, 
      you MUST add a population filter to exclude NULL results from the joined table
    - Example: After joining to a sanctions list, filter WHERE sanctions_list.id IS NOT NULL to only check matched records

11. **CRITICAL - Date Math:**
    - For date comparisons like "within X days", use TemporalDateMathAssertion
    - assertion_type: "temporal_date_math"
    - Example: Check if EDD completed within 14 days of onboarding
      {{
        "assertion_type": "temporal_date_math",
        "base_date_field": "edd_completion_date",
        "operator": "lte",
        "target_date_field": "onboarding_date",
        "offset_days": 14
      }}

12. **CRITICAL - Column-to-Column Comparisons:**
    - For comparing two dynamic columns (not static values), use ColumnComparisonAssertion
    - assertion_type: "column_comparison"
    - Example: Check if trade_date > clearance_date
      {{
        "assertion_type": "column_comparison",
        "left_field": "trade_date",
        "operator": "gt",
        "right_field": "clearance_date"
      }}

13. **Case-Insensitive String Matching:**
    - ValueMatchAssertion has "ignore_case_and_space": true by default
    - This handles variations like "APPROVED" vs "Approved" vs " approved "
    - Only set to false if exact case matching is required

14. **CRITICAL - Filter vs. Assertion Trap (MUST READ):**
    - The Population pipeline determines WHO gets tested. Assertions determine WHO passes.
    - If a missing record or NULL value is considered an audit violation, you MUST put the IS NOT NULL check in the Assertions block, NOT in the Population pipeline.
    - Example BAD: Putting "filter_is_null: false on assessment_status" in Population - this silently excludes vendors without assessments!
    - Example GOOD: Put "value_match: assessment_status IS NOT NULL" in Assertions - this catches vendors missing assessments as failures.
    - The Population pipeline is ONLY for scoping the dataset (e.g., "only check ACTIVE contracts"), never for passing/failing records.

15. **CRITICAL - Right-Side Join Key Checking:**
    - After a LEFT JOIN, ONLY the right-side JOIN KEYS are aliased with a "{{step_id}}_" prefix.
    - To check if a LEFT JOIN successfully matched, use: {{step_id}}_{{join_key}} IS NOT NULL
    - To check if a LEFT JOIN failed to match (anti-join pattern), use: {{step_id}}_{{join_key}} IS NULL
    - For ALL OTHER columns from the right table (non-key fields), use the column name WITHOUT any prefix
    - Example: After joining personal_trade_blotter to wall_cross_register (step_id: "join_wall_cross_register") on employee_id and ticker_symbol:
      * Use "join_wall_cross_register_employee_id IS NOT NULL" to verify the join matched
      * Use "restriction_status" (NOT "join_wall_cross_register_restriction_status") to check the actual restriction status field
    - Example: After joining hr_terminations to system_accounts (step_id: "join_system_accounts") on employee_id:
      * Use "join_system_accounts_employee_id IS NULL" to verify the employee does NOT exist in system_accounts

16. **CRITICAL - Handling IF/THEN Conditional Logic:**
    - If a control contains conditional logic (e.g., "If status is X, then verify Y"), you MUST decompose it:
      * The "IF" portion becomes a FilterComparison step in the Population pipeline to isolate target rows
      * The "THEN" portion becomes assertions in the Assertions array
    - Example: "If reconciliation_status is MISMATCH, then verify bref_raised_flag = Y and resolution within 30 days"
      * Population Filter: reconciliation_status = 'MISMATCH'
      * Assertion 1: bref_raised_flag = 'Y'
      * Assertion 2: TemporalDateMath(resolution_date <= application_date + 30)

ALLOWED OPERATORS:
- Filter: eq, neq, gt, lt, gte, lte
- ValueMatch: eq, neq, gt, lt, gte, lte, in, not_in
- Aggregation: gt, lt, eq, gte, lte
- ColumnComparison: eq, neq, gt, lt, gte, lte
- TemporalDateMath: gt, lt, eq, gte, lte

ASSERTION TYPES AVAILABLE:
- value_match: Compare field to static value
- column_comparison: Compare two fields to each other
- temporal_date_math: Compare date field to another date + offset
- temporal_sequence: Ensure chronological order of events
- aggregation: SUM/COUNT/AVG/MIN/MAX with HAVING clause
- aggregation_sum: (DEPRECATED - use aggregation instead)

EXAMPLE:
If control says "Ensure all trades over $10k are approved":
- Create FilterComparison step: notional_usd > 10000
- Create ValueMatchAssertion: approval_status == "APPROVED"

If control says "Employee can't trade within 30 days of wall-cross":
- Create JoinLeft with composite keys: ["employee_id", "ticker_symbol"]
- Create TemporalDateMathAssertion: trade_date >= wall_cross_date + 30 days
"""


class AITranslator:
    """
    AI-powered DSL translator with schema pruning.
    Fully leverages Instructor for self-correction loops and guaranteed JSON.
    """

    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-chat"):
        self.model = model
        logger.info(f"Initializing AITranslator with model: {model}")

        try:
            import instructor
            from openai import OpenAI
        except ImportError:
            logger.error("Instructor library not installed")
            raise ImportError(
                "Instructor library not installed. "
                "Run: pip install instructor openai pydantic"
            )

        # Initialize DeepSeek client with Instructor (OpenAI-compatible API)
        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            logger.error("DeepSeek API key not provided")
            raise ValueError(
                "DeepSeek API key required. Set DEEPSEEK_API_KEY environment variable "
                "or pass api_key parameter."
            )

        # Patch the OpenAI client with Instructor
        logger.debug("Initializing OpenAI client with Instructor patch")
        self.client = instructor.from_openai(
            OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1"),
            mode=instructor.Mode.JSON,
        )
        logger.info("AITranslator initialized successfully")

    def translate_control(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> EnterpriseControlDSL:
        """
        Two-pass translation with Pydantic self-correction.

        Args:
            control_text: Plain English control procedure
            evidence_headers: Dict mapping dataset_alias -> list of column names

        Returns:
            Validated EnterpriseControlDSL
        """
        logger.info("Starting two-pass AI translation")
        logger.debug(f"Control text length: {len(control_text)} chars")
        logger.debug(f"Evidence datasets: {list(evidence_headers.keys())}")

        try:
            # Pass 1: Schema pruning (Returns guaranteed Pydantic object)
            logger.debug("Starting Pass 1: Schema pruning")
            pruned_schema_obj = self._prune_schema(control_text, evidence_headers)
            logger.info(
                f"Schema pruned to {len(pruned_schema_obj.required_columns)} columns"
            )
            logger.debug(f"Pruned columns: {pruned_schema_obj.required_columns}")

            # Re-map the flat list back to dataset architecture
            pruned_columns = {}
            for col_ref in pruned_schema_obj.required_columns:
                if "." in col_ref:
                    dataset, col = col_ref.split(".", 1)
                    if dataset not in pruned_columns:
                        pruned_columns[dataset] = []
                    pruned_columns[dataset].append(col)

            # Pass 2: Generate DSL
            # Instructor handles the retries and feeds validation errors back to the LLM automatically!
            logger.debug("Starting Pass 2: DSL generation")
            dsl = self._generate_dsl(control_text, pruned_columns, evidence_headers)
            logger.info(
                f"DSL generation successful: control_id={dsl.governance.control_id}"
            )
            return dsl

        except ValidationError as e:
            logger.error(
                f"Pydantic validation error during translation: {e}", exc_info=True
            )
            logger.error(f"Validation errors: {e.errors()}")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error during AI translation: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise

    def _prune_schema(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> PrunedSchema:
        """First LLM pass: Identify relevant columns using strict Pydantic extraction."""
        logger.debug("Executing schema pruning pass")
        all_columns = []
        for dataset, cols in evidence_headers.items():
            all_columns.extend([f"{dataset}.{col}" for col in cols])

        logger.debug(f"Total available columns: {len(all_columns)}")

        prompt = PRUNING_PROMPT.format(
            control_text=control_text, all_column_names=", ".join(all_columns)
        )

        try:
            # Use Instructor to guarantee the output matches PrunedSchema
            result = self.client.chat.completions.create(
                model=self.model,
                response_model=PrunedSchema,  # <--- FIX: Use Pydantic here
                messages=[
                    {"role": "system", "content": "You are a banking data architect."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=500,
                temperature=0.1,
                max_retries=2,  # Automatic self-correction if it hallucinates format
            )
            logger.debug(
                f"Schema pruning completed: {len(result.required_columns)} columns selected"
            )
            return result
        except Exception as e:
            logger.error(
                f"Schema pruning failed: {type(e).__name__}: {e}", exc_info=True
            )
            raise

    def _generate_dsl(
        self,
        control_text: str,
        pruned_columns: Dict[str, List[str]],
        evidence_headers: Dict[str, List[str]],
    ) -> EnterpriseControlDSL:
        """
        Second LLM pass: Generate DSL with Pydantic validation and auto-retry.
        """
        logger.debug("Executing DSL generation pass")
        logger.debug(f"Pruned columns for DSL: {pruned_columns}")

        prompt = DSL_GENERATION_PROMPT.format(
            control_text=control_text,
            selected_columns_with_types=json.dumps(pruned_columns, indent=2),
            dataset_aliases=list(evidence_headers.keys()),
        )

        try:
            # Let Instructor handle the heavy lifting.
            # If the LLM generates an invalid operator, Instructor will catch it,
            # append the error to the prompt, and try again up to 3 times.
            logger.debug(f"Calling LLM for DSL generation (model: {self.model})")
            result = self.client.chat.completions.create(
                model=self.model,
                response_model=EnterpriseControlDSL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a compliance control compiler.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=4000,
                temperature=0.1,
                max_retries=3,  # <--- FIX: This replaces your manual try/except loop!
            )
            logger.debug("DSL generation successful")
            logger.debug(
                f"Generated DSL: control_id={result.governance.control_id}, "
                f"assertions={len(result.assertions)}, population_steps={len(result.population.steps)}"
            )
            return result

        except ValidationError as e:
            logger.error(
                f"Pydantic validation failed during DSL generation: {e}", exc_info=True
            )
            logger.error(
                f"Validation error details: {json.dumps(e.errors(), indent=2)}"
            )
            # Re-raise with more context
            raise ValueError(f"AI generated invalid DSL structure: {e}") from e
        except KeyError as e:
            logger.error(f"KeyError during DSL generation: {e}", exc_info=True)
            logger.error(
                "This may indicate the AI returned a wrapped or malformed JSON structure"
            )
            # Try to provide more helpful error message
            raise ValueError(
                f"AI response parsing failed with KeyError: {e}. "
                "The AI may have wrapped the DSL in an extra JSON layer. "
                "Check the prompt and response format."
            ) from e
        except Exception as e:
            logger.error(
                f"Unexpected error during DSL generation: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise

    def heal_dsl(
        self,
        original_dsl: EnterpriseControlDSL,
        sql_error_msg: str,
        evidence_headers: Dict[str, List[str]],
    ) -> EnterpriseControlDSL:
        """
        AI Self-Healing: Only triggered when DuckDB rejects compiled SQL.

        This is EXECUTION-GUIDED SELF-CORRECTION:
        - The AI acts as a Junior Developer
        - DuckDB is the strict judge that provides the stack trace
        - We feed the exact error back to force a targeted fix

        This preserves determinism because:
        1. AI never decides pass/fail (DuckDB does)
        2. AI only runs if deterministic validation fails
        3. The error message provides concrete guidance

        Args:
            original_dsl: The DSL that generated invalid SQL
            sql_error_msg: The exact DuckDB error message
            evidence_headers: Available schema for reference

        Returns:
            Corrected EnterpriseControlDSL
        """
        logger.warning(
            f"Triggering AI self-healing for control {original_dsl.governance.control_id}"
        )
        logger.debug(f"DuckDB error: {sql_error_msg}")

        healing_prompt = f"""
You are a SQL debugging assistant. Your previous DSL generated SQL that crashed the DuckDB execution engine.

DUCKDB ERROR MESSAGE:
{sql_error_msg}

AVAILABLE SCHEMA:
{json.dumps(evidence_headers, indent=2)}

ORIGINAL BROKEN DSL:
{original_dsl.model_dump_json(indent=2)}

TASK:
Identify why the SQL failed based on the error message (e.g., referencing a column that doesn't exist, 
ambiguous column in a join, wrong data type). Return a corrected EnterpriseControlDSL JSON that resolves 
this exact error.

CRITICAL RULES:
1. Only fix the specific issue mentioned in the error
2. Use EXACT column names from the available schema
3. For joins, ensure columns are properly qualified with dataset aliases
4. Do NOT invent new operators or fields
5. Return the FULL corrected DSL, not just the changed parts
"""

        try:
            logger.debug("Calling LLM for DSL healing")
            corrected_dsl = self.client.chat.completions.create(
                model=self.model,
                response_model=EnterpriseControlDSL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a strict compliance control compiler fixing a database error.",
                    },
                    {"role": "user", "content": healing_prompt},
                ],
                max_tokens=4000,
                temperature=0.1,
                max_retries=2,
            )
            logger.info(
                f"AI self-healing completed for control {corrected_dsl.governance.control_id}"
            )
            return corrected_dsl

        except Exception as e:
            logger.error(
                f"AI self-healing failed: {type(e).__name__}: {e}", exc_info=True
            )
            # Re-raise - if healing fails, we can't proceed
            raise RuntimeError(
                f"AI self-healing failed to fix DSL: {type(e).__name__}: {str(e)}"
            ) from e


class MockAITranslator:
    """
    Mock translator for testing without API calls.
    Returns a sample DSL based on control text patterns.
    """

    def translate_control(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> EnterpriseControlDSL:
        """Returns a mock DSL for testing"""
        logger.info("MockAITranslator called (no real API calls)")
        logger.debug(f"Control text: {control_text[:100]}...")

        # Simple pattern matching for demo
        base_dataset = list(evidence_headers.keys())[0]
        columns = evidence_headers[base_dataset]
        logger.debug(f"Using base dataset: {base_dataset} with {len(columns)} columns")

        # Create a simple value match assertion
        mock_dsl_dict = {
            "governance": {
                "control_id": "MOCK-001",
                "version": "1.0.0",
                "owner_role": "Compliance",
                "testing_frequency": "Daily",
                "regulatory_citations": ["SOX 404"],
                "risk_objective": control_text[:100],
            },
            "ontology_bindings": [
                {
                    "business_term": columns[0],
                    "dataset_alias": base_dataset,
                    "technical_field": columns[0],
                    "data_type": "string",
                }
            ],
            "population": {"base_dataset": base_dataset, "steps": [], "sampling": None},
            "assertions": [
                {
                    "assertion_id": "assert_001",
                    "assertion_type": "value_match",
                    "description": "Mock assertion",
                    "field": columns[0],
                    "operator": "eq",
                    "expected_value": "APPROVED",
                    "materiality_threshold_percent": 0.0,
                }
            ],
            "evidence": {
                "retention_years": 7,
                "reviewer_workflow": "Requires_Human_Signoff",
                "exception_routing_queue": "JIRA:COMPLIANCE",
            },
        }

        return EnterpriseControlDSL(**mock_dsl_dict)

    def heal_dsl(
        self,
        original_dsl: EnterpriseControlDSL,
        sql_error_msg: str,
        evidence_headers: Dict[str, List[str]],
    ) -> EnterpriseControlDSL:
        """Mock healing: just return the original DSL (no real healing in mock mode)"""
        logger.info("MockAITranslator.heal_dsl called (no real API calls)")
        logger.debug(f"Mock healing for control: {original_dsl.governance.control_id}")
        logger.debug(f"Error message: {sql_error_msg[:100]}...")

        # In mock mode, we can't actually fix anything, so just return original
        # In real tests, use a real AITranslator
        return original_dsl
