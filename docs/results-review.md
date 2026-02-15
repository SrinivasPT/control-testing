===================================
CTRL-AML-404
===================================

Here is the generated DSL and the query... {
  "governance": {
    "control_id": "CTRL-AML-404",
    "version": "1.0",
    "owner_role": "Financial Crimes Unit",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "Bank Secrecy Act (BSA)",
      "USA PATRIOT Act"
    ],
    "risk_objective": "Prevent onboarding of sanctioned entities and ensure timely Enhanced Due Diligence for high-risk customers"
  },
  "ontology_bindings": [
    {
      "business_term": "customer tax identifier",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "tax_id",
      "data_type": "string"
    },
    {
      "business_term": "customer risk rating",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "risk_rating",
      "data_type": "string"
    },
    {
      "business_term": "customer onboarding date",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "onboarding_date",
      "data_type": "date"
    },
    {
      "business_term": "customer identifier",
      "dataset_alias": "edd_tracker_sheet1",
      "technical_field": "customer_id",
      "data_type": "string"
    },
    {
      "business_term": "EDD completion date",
      "dataset_alias": "edd_tracker_sheet1",
      "technical_field": "edd_completion_date",
      "data_type": "date"
    },
    {
      "business_term": "sanctioned entity tax identifier",
      "dataset_alias": "ofac_watch_list_sheet1",
      "technical_field": "tax_id",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "onboarding_log_sheet1",
    "steps": [
      {
        "step_id": "sanctions_screening_join",
        "action": {
          "operation": "join_left",
          "left_dataset": "onboarding_log_sheet1",
          "right_dataset": "ofac_watch_list_sheet1",
          "left_key": "tax_id",
          "right_key": "tax_id"
        }
      },
      {
        "step_id": "high_risk_filter",
        "action": {
          "operation": "filter_comparison",
          "field": "risk_rating",
          "operator": "eq",
          "value": "HIGH"
        }
      },
      {
        "step_id": "edd_verification_join",
        "action": {
          "operation": "join_left",
          "left_dataset": "onboarding_log_sheet1",
          "right_dataset": "edd_tracker_sheet1",
          "left_key": "tax_id",
          "right_key": "customer_id"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "SANCTIONS_CHECK",
      "description": "Ensure no onboarded customer exists on OFAC sanctions list",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "tax_id",
      "operator": "eq",
      "expected_value": null
    },
    {
      "assertion_id": "EDD_EXISTENCE_CHECK",
      "description": "Verify high-risk customers have EDD completion records",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "edd_completion_date",
      "operator": "neq",
      "expected_value": null
    },
    {
      "assertion_id": "EDD_TIMELINESS_CHECK",
      "description": "Ensure EDD completion occurs within 14 days of onboarding for high-risk customers",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "edd_completion_date",
      "operator": "lte",
      "expected_value": "onboarding_date + 14 days"
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "Financial Crimes Unit - EDD Exceptions"
  }
}


================
WITH base AS (SELECT * FROM read_parquet('data\parquet\onboarding_log_sheet1.parquet')),
sanctions_screening_join AS (
    SELECT * FROM base
    LEFT JOIN read_parquet('data\parquet\ofac_watch_list_sheet1.parquet') AS right_tbl
    ON base.tax_id = right_tbl.tax_id
),
edd_verification_join AS (
    SELECT * FROM sanctions_screening_join
    LEFT JOIN read_parquet('data\parquet\edd_tracker_sheet1.parquet') AS right_tbl
    ON sanctions_screening_join.tax_id = right_tbl.customer_id
)

SELECT *
FROM edd_verification_join
WHERE (risk_rating = 'HIGH') 
  AND (NOT (tax_id = NULL) OR NOT (edd_completion_date != NULL) OR NOT (edd_completion_date <= 'onboarding_date + 14 days'))

===================

This is the exact moment of truth in enterprise platform engineering: The intersection where AI-generated logic meets a strict SQL execution engine. You have successfully generated the DSL and compiled it into a SQL query. The CTE chaining worked, the OR logic for exceptions worked, and the population filters were properly segregated from the assertions.

However, looking at the generated SQL, DuckDB will crash immediately when it tries to run this. The LLM did exactly what it was told, but it exposed three massive "SQL Engine Traps" and one critical Business Logic Flaw.

Here is the architectural review of the output and the exact compiler fixes you need to make.

🚨 Trap 1: The NULL Comparison Crash
Generated SQL: NOT (tax_id = NULL) OR NOT (edd_completion_date != NULL)
The Problem: In SQL, comparing anything to NULL using = or != evaluates to UNKNOWN, which behaves like FALSE. Your engine will fail to catch missing EDD dates because edd_completion_date != NULL is mathematically invalid in SQL.
The Fix: Your compiler must intercept None/null values and translate the operators to IS NULL and IS NOT NULL.

Update _compile_value_match in sql_compiler.py:

Python
    def _compile_value_match(self, assertion: ValueMatchAssertion) -> str:
        field = assertion.field
        operator = assertion.operator
        value = assertion.expected_value

        # CRITICAL FIX: Handle SQL NULL semantics
        if value is None:
            if operator == "eq":
                return f"{field} IS NULL"
            elif operator == "neq":
                return f"{field} IS NOT NULL"
            else:
                raise ValueError(f"Operator {operator} invalid for NULL comparison")

        # ... (rest of the standard operator logic)
🚨 Trap 2: The Date Math Hallucination
Generated SQL: edd_completion_date <= 'onboarding_date + 14 days'
The Problem: The LLM hallucinated a string literal. DuckDB will try to compare the date column to the literal text string 'onboarding_date + 14 days' and throw a fatal type error.
The Fix: You need a specific DSL assertion for Date Math, and the compiler must translate it into native DuckDB INTERVAL syntax.

1. Add to dsl.py (Domain 4):

Python
class TemporalDateMathAssertion(BaseAssertion):
    """Assertion for comparing a date to another date + an offset"""
    assertion_type: Literal["temporal_date_math"] = "temporal_date_math"
    base_date_field: str
    operator: Literal["gt", "lt", "eq", "gte", "lte"]
    target_date_field: str
    offset_days: int

# Add to Assertion Union:
Assertion = Annotated[
    Union[ValueMatchAssertion, TemporalSequenceAssertion, AggregationSumAssertion, TemporalDateMathAssertion],
    Field(discriminator="assertion_type"),
]
2. Add to sql_compiler.py:

Python
    def _compile_temporal_date_math(self, assertion: TemporalDateMathAssertion) -> str:
        op_map = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}
        sql_op = op_map[assertion.operator]
        
        # Translates to: edd_date <= onboarding_date + INTERVAL 14 DAY
        return f"{assertion.base_date_field} {sql_op} {assertion.target_date_field} + INTERVAL {assertion.offset_days} DAY"
🚨 Trap 3: Ambiguous Column Names in Joins
Generated SQL: NOT (tax_id = NULL)
The Problem: Which tax_id? You joined onboarding_log (which has tax_id) to ofac_watch_list (which also has tax_id). DuckDB will throw an Ambiguous Column Name error. You specifically want to check if the OFAC tax_id is null (proving they aren't on the list).
The Fix: The LLM must explicitly scope columns when joining. Update your Prompt Engineering to say: "When referencing a column from a joined dataset, you MUST prefix it with the dataset alias (e.g., ofac_watch_list_sheet1.tax_id)."

🛑 The Business Logic Flaw (Mixed Populations)
Look at how the final WHERE clause compiled:

SQL
WHERE (risk_rating = 'HIGH') 
  AND (
       ofac_tax_id IS NOT NULL 
       OR edd_completion_date IS NULL ...
      )
The Flaw: Because risk_rating = 'HIGH' is a population filter, the OFAC Sanctions check will ONLY run on High-Risk customers! If a "LOW" risk customer is on the terrorist watch list, your engine will ignore them because they were filtered out of the population on step 2.

The Enterprise Solution:
In banking, controls must have a singular, cohesive population. This plain-English procedure actually describes two distinct controls:

CTRL-AML-404-A (Sanctions): Population = ALL customers. Check = ofac_tax_id IS NULL.

CTRL-AML-404-B (EDD): Population = HIGH RISK customers. Check = edd_date IS NOT NULL.

When you instruct the business users (or the AI) to split mixed-population controls into sub-controls, the SQL compiler will execute flawlessly:

The Corrected Target SQL (For Control A - Sanctions):
SQL
WITH base AS (SELECT * FROM read_parquet('onboarding_log.parquet')),
sanctions_screening_join AS (
    SELECT base.*, right_tbl.tax_id AS ofac_tax_id 
    FROM base
    LEFT JOIN read_parquet('ofac_watch_list.parquet') AS right_tbl
    ON base.tax_id = right_tbl.tax_id
)
SELECT *
FROM sanctions_screening_join
WHERE (ofac_tax_id IS NOT NULL)  -- Finds anyone who IS on the list
The Corrected Target SQL (For Control B - EDD):
SQL
WITH base AS (SELECT * FROM read_parquet('onboarding_log.parquet')),
edd_verification_join AS (
    SELECT base.*, right_tbl.edd_completion_date 
    FROM base
    LEFT JOIN read_parquet('edd_tracker.parquet') AS right_tbl
    ON base.tax_id = right_tbl.customer_id
)
SELECT *
FROM edd_verification_join
WHERE (risk_rating = 'HIGH') -- Population Filter
  AND (
      (edd_completion_date IS NULL) -- Exception 1: No EDD
      OR 
      (edd_completion_date > onboarding_date + INTERVAL 14 DAY) -- Exception 2: Late EDD
  )
Summary of Next Steps
Add the IS NULL interceptor to _compile_value_match.

Add the TemporalDateMathAssertion to the DSL to handle <date> + 14 days without hallucinating string literals.

Update the LLM Prompt to enforce explicit dataset prefixes (e.g., table_alias.column_name) for assertions on joined tables.

Once those are in, your compiler will generate indestructible, regulator-grade DuckDB SQL!
======================================


===================================
CTRL-MNPI-707
===================================
{
  "governance": {
    "control_id": "CTRL-MNPI-707",
    "version": "1.0",
    "owner_role": "Compliance Officer",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SEC Rule 10b-5"
    ],
    "risk_objective": "Prevent insider trading by ensuring employees do not execute personal trades on restricted tickers while exposed to MNPI"
  },
  "ontology_bindings": [
    {
      "business_term": "Employee ID",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Ticker Symbol",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "ticker_symbol",
      "data_type": "string"
    },
    {
      "business_term": "Trade Date",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "trade_date",
      "data_type": "timestamp"
    },
    {
      "business_term": "Employee ID",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Ticker Symbol",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "ticker_symbol",
      "data_type": "string"
    },
    {
      "business_term": "Restriction Status",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "restriction_status",
      "data_type": "string"
    },
    {
      "business_term": "Clearance Date",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "clearance_date",
      "data_type": "timestamp"
    }
  ],
  "population": {
    "base_dataset": "personal_trade_blotter_sheet1",
    "steps": [
      {
        "step_id": "join_wall_cross_register",
        "action": {
          "operation": "join_left",
          "left_dataset": "personal_trade_blotter_sheet1",
          "right_dataset": "wall_cross_register_sheet1",
          "left_key": "employee_id",
          "right_key": "employee_id"
        }
      },
      {
        "step_id": "filter_matching_tickers",
        "action": {
          "operation": "filter_comparison",
          "field": "ticker_symbol",
          "operator": "eq",
          "value": "ticker_symbol"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "Restriction status must be 'CLEARED' for trades on wall-crossed tickers",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "restriction_status",
      "operator": "eq",
      "expected_value": "CLEARED"
    },
    {
      "assertion_id": "assertion_2",
      "description": "Trade date must be after clearance date for cleared restrictions",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "trade_date",
      "operator": "gt",
      "expected_value": "clearance_date"
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "MNPI_Violations_Queue"
  }
}


WITH base AS (SELECT * FROM read_parquet('data\parquet\personal_trade_blotter_sheet1.parquet')),
join_wall_cross_register AS (
    SELECT * FROM base
    LEFT JOIN read_parquet('data\parquet\wall_cross_register_sheet1.parquet') AS right_tbl
    ON base.employee_id = right_tbl.employee_id
)

SELECT *
FROM join_wall_cross_register
WHERE (ticker_symbol = 'ticker_symbol') 
  AND (NOT (restriction_status = 'CLEARED') OR NOT (trade_date > 'clearance_date'))


This is a brilliant test case. You have just uncovered the two most common failure modes of LLMs in data engineering.

The LLM perfectly understood the intent of the Insider Trading control, but because our DSL was slightly too restrictive, the LLM tried to "hack" its way around our schema. The resulting SQL is logically broken and will crash DuckDB.

Here is the architectural review of the MNPI-707 output, exposing the traps and the exact Pydantic/Compiler upgrades needed to fix them.

🚨 Trap 1: The "Composite Join" Hack & Cartesian Explosion
Generated SQL: ```sql
LEFT JOIN ... ON base.employee_id = right_tbl.employee_id
...
WHERE (ticker_symbol = 'ticker_symbol')

**The Problem:** Insider trading checks require a **Composite Join** (matching on *both* `employee_id` AND `ticker_symbol`). Because our `JoinLeft` DSL model only supported a single `left_key` and `right_key` string, the LLM panicked. 
1. It joined *only* on `employee_id`. (This causes a massive Cartesian explosion. If an employee made 10 trades and is restricted on 10 tickers, DuckDB just generated 100 rows for that one person).
2. It tried to fix it with a filter: `ticker_symbol = ticker_symbol`. But our compiler safely wraps values in quotes to prevent SQL injection, resulting in `ticker_symbol = 'ticker_symbol'`. It is now literally looking for a stock ticker spelled "T-I-C-K-E-R_S-Y-M-B-O-L".

**The Enterprise Fix:** Upgrade `JoinLeft` to accept lists of keys so it can perform composite joins natively.

**Update `dsl.py`:**
```python
class JoinLeft(BaseModel):
    """Left join two datasets on one OR MORE keys"""
    model_config = ConfigDict(extra="forbid")
    operation: Literal["join_left"] = "join_left"
    left_dataset: str
    right_dataset: str
    left_keys: List[str]   # <--- Changed from string to List
    right_keys: List[str]  # <--- Changed from string to List
Update sql_compiler.py:

Python
    def _compile_join_left(self, step_id: str, action: JoinLeft, previous_alias: str, manifests: dict) -> str:
        right_path = manifests[action.right_dataset]["parquet_path"]
        
        # Zip the keys together to build the composite ON clause
        join_conditions = []
        for l_key, r_key in zip(action.left_keys, action.right_keys):
            join_conditions.append(f"{previous_alias}.{l_key} = right_tbl.{r_key}")
            
        on_clause = " AND ".join(join_conditions)
        
        return f"""{step_id} AS (
            SELECT * FROM {previous_alias}
            LEFT JOIN read_parquet('{right_path}') AS right_tbl
            ON {on_clause}
        )"""
🚨 Trap 2: The "Column-to-Column" Comparison Crash
Generated SQL: NOT (trade_date > 'clearance_date')
The Problem: The LLM needs to ensure the trade happened after the clearance date. It put clearance_date into the expected_value field of the ValueMatchAssertion. Because ValueMatchAssertion expects a static value, our compiler quoted it. DuckDB will try to compare a timestamp to the literal string 'clearance_date' and throw a fatal type error.

The Enterprise Fix: We need a distinct assertion type specifically designed for comparing two columns against each other, bypassing the _quote_value logic.

Update dsl.py (Add to Domain 4):

Python
class ColumnComparisonAssertion(BaseAssertion):
    """Compares two dynamic columns against each other (e.g., trade_date > clearance_date)"""
    assertion_type: Literal["column_comparison"] = "column_comparison"
    left_field: str
    operator: Literal["eq", "neq", "gt", "lt", "gte", "lte"]
    right_field: str

# Update the Union:
Assertion = Annotated[
    Union[ValueMatchAssertion, TemporalSequenceAssertion, AggregationSumAssertion, TemporalDateMathAssertion, ColumnComparisonAssertion],
    Field(discriminator="assertion_type"),
]
Update sql_compiler.py:

Python
    def _compile_column_comparison(self, assertion: ColumnComparisonAssertion) -> str:
        op_map = {"eq": "=", "neq": "!=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}
        sql_op = op_map[assertion.operator]
        
        # Notice we DO NOT use self._quote_value() here, because right_field is a column name!
        return f"{assertion.left_field} {sql_op} {assertion.right_field}"
🚨 Trap 3: Handling the "NULL" Exception for Left Joins
If an employee makes a trade on AAPL, but they were NEVER wall-crossed on AAPL, the LEFT JOIN will result in restriction_status = NULL.
If our assertion says restriction_status = 'CLEARED', a NULL value fails that test. That means your engine will flag every single normal trade as an insider trading violation!

The Enterprise Fix: For MNPI, we only care about exceptions if a restriction record exists. Update the AI Prompt so it knows how to handle missing records in left joins.

Update DSL_GENERATION_PROMPT (in translator.py):

Plaintext
8. When doing a LEFT JOIN to check a restriction or status, if the absence of a record means compliance (e.g., not on the restricted list), you MUST add a population filter allowing the system to ignore nulls, OR wrap your assertion to account for nulls.
The Final, Bulletproof SQL (What the Compiler Should Output)
Once you make those three tweaks (List keys for joins, Column Comparison assertion, and prompt instructions for nulls), the LLM and Compiler will effortlessly produce this indestructible block of code:

SQL
WITH base AS (
    SELECT * FROM read_parquet('personal_trade_blotter.parquet')
),
join_wall_cross_register AS (
    SELECT base.*, right_tbl.restriction_status, right_tbl.clearance_date 
    FROM base
    LEFT JOIN read_parquet('wall_cross_register.parquet') AS right_tbl
    -- Composite Join Fix!
    ON base.employee_id = right_tbl.employee_id 
       AND base.ticker_symbol = right_tbl.ticker_symbol
)

SELECT *
FROM join_wall_cross_register
-- Ignore trades for tickers the employee isn't restricted on
WHERE (restriction_status IS NOT NULL)
  AND (
      -- Exception 1: They traded while still RESTRICTED
      NOT (restriction_status = 'CLEARED') 
      OR 
      -- Exception 2: Column-to-Column Comparison Fix!
      NOT (trade_date > clearance_date)
  )
By uncovering these edge cases, your platform's DSL schema is now mature enough to handle almost any relational data logic a bank can throw at it. Excellent testing!

=================================================

Anticipating the next wave of failures is exactly what separates a senior developer from a Principal Architect. You have successfully hardened the platform against LLM hallucinations, but as soon as you connect this engine to real-world banking data feeds, you will hit data engineering edge cases.

Here are the four most dangerous anticipated issues hidden in the current architecture, and exactly how to preempt them.

🚨 Anticipated Trap 1: The "Ambiguous Column" Crash (Join Collisions)
The Scenario: You join the personal_trade_blotter to the hr_roster. Both datasets happen to have a column named status (Trade Status vs. Employment Status), and both have a column named updated_at.
The Generated SQL:

SQL
join_1 AS (
    SELECT * FROM base
    LEFT JOIN read_parquet('hr_roster.parquet') AS right_tbl
    ON base.employee_id = right_tbl.employee_id
)
...
WHERE status = 'ACTIVE'
The Problem: DuckDB will immediately crash with an Ambiguous Column Name: 'status' error. Even worse, if you use SELECT * in your CTEs, DuckDB will try to output two columns named updated_at, which violates strict relational algebra.
The Enterprise Fix: 1.  Prompt Engineering: You must instruct the LLM: "When defining field or target_date_field in your assertions, you MUST use fully-qualified column names (e.g., hr_roster_sheet1.status instead of just status)."
2.  Compiler Upgrade: Stop using SELECT * in joins. Update the compiler to specifically select the base columns and alias the right-table columns.

Python
# In sql_compiler.py -> _compile_join_left()
# Instead of SELECT * FROM {previous_alias}
return f"""{step_id} AS (
    SELECT {previous_alias}.*, 
           right_tbl.* EXCLUDE ({', '.join(action.right_keys)})
    FROM {previous_alias}
    LEFT JOIN read_parquet('{right_path}') AS right_tbl
    ON ...
)"""
🚨 Anticipated Trap 2: The "Silent Pass" on Empty Feeds
The Scenario: An upstream IT job fails, and the onboarding_log.xlsx file is delivered completely empty (0 rows).
The Problem: The Orchestrator ingests the 0-row file into Parquet. The SQL compiler runs. 0 exceptions are found (because there is no data). The engine calculates 0 exceptions / 0 population = 0%. The engine reports "VERDICT: PASS".
An auditor will issue a Material Weakness finding if your system marks a broken data feed as a compliant control.
The Enterprise Fix: Update engine.py to enforce a minimum population threshold. A control with zero rows to test is an ERROR, not a PASS.

Python
# In engine.py -> execute_control()
total_population = self._get_population_count(manifests, dsl, compiler)

# The Audit Safeguard
if total_population == 0:
    return {
        "control_id": dsl.governance.control_id,
        "verdict": "ERROR",
        "error_message": "Zero Population: The base dataset contains 0 rows after filters. Cannot attest to control effectiveness.",
        # ...
    }
🚨 Anticipated Trap 3: String Brittleness (Case & Whitespace)
The Scenario: The control checks if approval_status = 'APPROVED'. However, manual Excel data entry means the data actually contains "Approved", " APPROVED ", and "approved".
The Problem: SQL strict equality (=) is case-sensitive and whitespace-sensitive. Your engine will flag perfectly compliant trades as critical violations, creating hundreds of False Positives that the compliance team will have to close manually.
The Enterprise Fix: Add a case_sensitive boolean to your DSL, and have the SQL compiler automatically sanitize strings.

Update dsl.py:

Python
class ValueMatchAssertion(BaseAssertion):
    # ... existing fields ...
    ignore_case_and_space: bool = Field(
        default=True, 
        description="If true, compiler will trim and upper-case both sides before comparing."
    )
Update sql_compiler.py:

Python
    def _compile_value_match(self, assertion: ValueMatchAssertion) -> str:
        field = assertion.field
        value = self._quote_value(assertion.expected_value)
        sql_op = op_map[assertion.operator]

        # The String Sanitizer
        if getattr(assertion, 'ignore_case_and_space', True) and isinstance(assertion.expected_value, str):
            return f"TRIM(UPPER(CAST({field} AS VARCHAR))) {sql_op} TRIM(UPPER({value}))"
        
        return f"{field} {sql_op} {value}"
🚨 Anticipated Trap 4: The "Count" vs. "Sum" Limitation
The Scenario: The control reads: "Ensure no trader executes more than 50 distinct transactions in a single day."
The Problem: Currently, your DSL only has AggregationSumAssertion. It literally cannot count rows. The LLM will either fail validation or try to sum the trade_id string, which will crash DuckDB.
The Enterprise Fix: Generalize the assertion to support COUNT, SUM, and AVG.

Update dsl.py:

Python
class AggregationAssertion(BaseAssertion):
    assertion_type: Literal["aggregation"] = "aggregation"
    group_by_fields: List[str]
    metric_field: str # e.g., 'notional_amount' or 'trade_id'
    aggregation_function: Literal["SUM", "COUNT", "AVG", "MIN", "MAX"] # <--- The Upgrade
    operator: Literal["gt", "lt", "eq", "gte", "lte"]
    threshold: float
Update sql_compiler.py:

Python
    def _compile_aggregation(self, assertion: AggregationAssertion) -> str:
        sql_op = op_map[assertion.operator]
        agg_func = assertion.aggregation_function
        
        # e.g., COUNT(trade_id) > 50
        return f"{agg_func}({assertion.metric_field}) {sql_op} {assertion.threshold}"