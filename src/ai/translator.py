"""
AI Translation Module
Schema pruning and DSL generation using OpenAI/Anthropic
"""

import os
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from src.models.dsl import EnterpriseControlDSL


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
      {
        "assertion_type": "temporal_date_math",
        "base_date_field": "edd_completion_date",
        "operator": "lte",
        "target_date_field": "onboarding_date",
        "offset_days": 14
      }

12. **CRITICAL - Column-to-Column Comparisons:**
    - For comparing two dynamic columns (not static values), use ColumnComparisonAssertion
    - assertion_type: "column_comparison"
    - Example: Check if trade_date > clearance_date
      {
        "assertion_type": "column_comparison",
        "left_field": "trade_date",
        "operator": "gt",
        "right_field": "clearance_date"
      }

13. **Case-Insensitive String Matching:**
    - ValueMatchAssertion has "ignore_case_and_space": true by default
    - This handles variations like "APPROVED" vs "Approved" vs " approved "
    - Only set to false if exact case matching is required

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

        try:
            import instructor
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "Instructor library not installed. "
                "Run: pip install instructor openai pydantic"
            )

        # Initialize DeepSeek client with Instructor (OpenAI-compatible API)
        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError(
                "DeepSeek API key required. Set DEEPSEEK_API_KEY environment variable "
                "or pass api_key parameter."
            )

        # Patch the OpenAI client with Instructor
        self.client = instructor.from_openai(
            OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1"),
            mode=instructor.Mode.JSON,
        )

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
        # Pass 1: Schema pruning (Returns guaranteed Pydantic object)
        pruned_schema_obj = self._prune_schema(control_text, evidence_headers)

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
        return self._generate_dsl(control_text, pruned_columns, evidence_headers)

    def _prune_schema(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> PrunedSchema:
        """First LLM pass: Identify relevant columns using strict Pydantic extraction."""
        all_columns = []
        for dataset, cols in evidence_headers.items():
            all_columns.extend([f"{dataset}.{col}" for col in cols])

        prompt = PRUNING_PROMPT.format(
            control_text=control_text, all_column_names=", ".join(all_columns)
        )

        # Use Instructor to guarantee the output matches PrunedSchema
        return self.client.chat.completions.create(
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

    def _generate_dsl(
        self,
        control_text: str,
        pruned_columns: Dict[str, List[str]],
        evidence_headers: Dict[str, List[str]],
    ) -> EnterpriseControlDSL:
        """
        Second LLM pass: Generate DSL with Pydantic validation and auto-retry.
        """
        import json  # Only used for dumping the dictionary to the prompt string

        prompt = DSL_GENERATION_PROMPT.format(
            control_text=control_text,
            selected_columns_with_types=json.dumps(pruned_columns, indent=2),
            dataset_aliases=list(evidence_headers.keys()),
        )

        # Let Instructor handle the heavy lifting.
        # If the LLM generates an invalid operator, Instructor will catch it,
        # append the error to the prompt, and try again up to 3 times.
        return self.client.chat.completions.create(
            model=self.model,
            response_model=EnterpriseControlDSL,
            messages=[
                {"role": "system", "content": "You are a compliance control compiler."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=4000,
            temperature=0.1,
            max_retries=3,  # <--- FIX: This replaces your manual try/except loop!
        )


class MockAITranslator:
    """
    Mock translator for testing without API calls.
    Returns a sample DSL based on control text patterns.
    """

    def translate_control(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> EnterpriseControlDSL:
        """Returns a mock DSL for testing"""

        # Simple pattern matching for demo
        base_dataset = list(evidence_headers.keys())[0]
        columns = evidence_headers[base_dataset]

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
