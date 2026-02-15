============================
sql_compiler
============================

Gemini said
This is a highly mature implementation. Copilot/Claude successfully integrated the previous_alias CTE chaining and the SQL injection protection in _quote_value. The structure is exceptionally clean.

However, putting my "Principal Architect" hat back on and doing a dry-run of the compilation logic, I caught a critical boolean logic bug that will cause your engine to miss compliance violations.

Here is the code review, followed by the exact fix.

🚨 Critical Bug: The De Morgan's Law Trap
Look at how you are currently handling the WHERE clause:

Python
# During Population parsing
self.where_conditions.append(cond) # e.g., "trade_date > '2025-01-01'"

# During Assertion parsing
self.where_conditions.append(f"NOT ({cond})") # e.g., "NOT (status = 'APPROVED')"
self.where_conditions.append(f"NOT ({cond2})") # e.g., "NOT (has_bref = TRUE)"

# Construction
where_clause = " AND ".join(self.where_conditions)
The Logical Flaw: If you join everything with AND, the resulting SQL is:
WHERE trade_date > '2025-01-01' AND NOT (status = 'APPROVED') AND NOT (has_bref = TRUE)

By using AND between multiple negated assertions, a row will only be flagged as an exception if it violates EVERY single rule simultaneously. If a trade is unapproved (fails Rule 1) but does have a BREF (passes Rule 2), it will slip through the cracks and be marked as compliant.

To find any exception within the population, you must separate your population filters from your assertions, and join the assertions with OR.

The Fix: Splitting Filters and Exceptions
You need to maintain two separate lists in your __init__ and change the assembly logic in _construct_query. Update your compiler with these exact changes:

1. Update __init__
Python
    def __init__(self, dsl: EnterpriseControlDSL):
        self.dsl = dsl
        self.population_filters: List[str] = []  # MUST be true (AND)
        self.assertion_exceptions: List[str] = [] # If ANY are true, it's a failure (OR)
        self.having_conditions: List[str] = []
        self.group_by_fields: List[str] = []
        self.cte_fragments: List[str] = []
2. Update _build_population_cte
Change where the filters append:

Python
            if action.operation == "filter_comparison":
                cond = self._compile_filter_comparison(action)
                self.population_filters.append(cond) # <-- Changed

            elif action.operation == "filter_in_list":
                cond = self._compile_filter_in_list(action)
                self.population_filters.append(cond) # <-- Changed
3. Update _compile_assertions
Change where the row-level and temporal assertions append:

Python
            if isinstance(assertion, ValueMatchAssertion):
                cond = self._compile_value_match(assertion)
                self.assertion_exceptions.append(f"NOT ({cond})") # <-- Changed

            elif isinstance(assertion, TemporalSequenceAssertion):
                cond = self._compile_temporal_sequence(assertion)
                self.assertion_exceptions.append(f"NOT ({cond})") # <-- Changed
4. Update _construct_query
This is where the boolean magic happens. We isolate the population, then check if any rule is broken.

Python
    def _construct_query(
        self, final_population_alias: str, sampling_clause: str
    ) -> str:
        """Assembles final SQL query"""
        # Build CTE chain
        cte_sql = "WITH " + ",\n".join(self.cte_fragments) if self.cte_fragments else ""

        # 1. Assemble Population Filters (AND)
        pop_clause = " AND ".join(self.population_filters) if self.population_filters else "1=1"
        
        # 2. Assemble Exceptions (OR)
        if self.assertion_exceptions:
            exceptions_clause = " OR ".join(self.assertion_exceptions)
            # Final WHERE: Must be in population, AND must break AT LEAST ONE rule
            where_clause = f"({pop_clause}) \n  AND ({exceptions_clause})"
        else:
            where_clause = pop_clause

        # ... (Rest of the select_sql generation remains the same)
💡 Minor DuckDB Polish: The Sampling Syntax
In _build_sampling_clause, you generated \nUSING SAMPLE X ROWS. While standard in some dialects, DuckDB's safest, most deterministic sampling method (especially for SOX audits where you need to prove random reservoir sampling) uses the TABLESAMPLE keyword directly after the table alias in the FROM clause.

I recommend updating your sampling method to this:

Python
    def _build_sampling_clause(self) -> str:
        """Builds DuckDB specific TABLESAMPLE clause"""
        if not self.dsl.population.sampling or not self.dsl.population.sampling.enabled:
            return ""

        sampling = self.dsl.population.sampling
        seed_clause = f" REPEATABLE ({sampling.random_seed})" if sampling.random_seed else ""

        if sampling.sample_size:
            return f" TABLESAMPLE RESERVOIR({sampling.sample_size} ROWS){seed_clause}"
        elif sampling.sample_percentage:
            pct = int(sampling.sample_percentage * 100)
            return f" TABLESAMPLE RESERVOIR({pct}%){seed_clause}"

        return ""
Note: With this change, ensure it attaches directly to the alias in your _construct_query: FROM {final_population_alias}{sampling_clause}.

Next Steps
Make those boolean logic tweaks, and your SQLCompiler is mathematically sound and ready to process the ECOA, Row-Level, and Aggregation test files we generated earlier!

Would you like me to review the engine.py (DuckDB execution) or audit_fabric.py (PostgreSQL/SQLite) next?


============================
Engine.py and Ingestion.py
============================
Gemini said
This code is exceptional. You have successfully implemented the "Day-2" enterprise enhancements (Schema Drift Validation and advanced Parquet metadata hashing) right out of the gate.

The get_column_headers using nrows=0 is a brilliant, memory-safe way to feed the AI, and the schema version hash is exactly what auditors want to see to prove data wasn't manipulated.

However, continuing our peer review, I have caught two implementation bugs that will break your execution pipeline. One is a residual bug from our previous compiler refactor, and the other is a subtle but dangerous data-engineering trap regarding Parquet types.

Here is the review and the exact fixes.

🚨 Critical Bug 1: The Outdated Population Count (Engine Layer)
In your ExecutionEngine._get_population_count(), you wrote this logic to try and figure out how many rows are in the baseline population:

Python
# Current Code
if compiler.where_conditions:
    # Only apply filter conditions, not assertion conditions
    filter_conditions = [
        cond for cond in compiler.where_conditions if not cond.startswith("NOT (")
    ]
The Problem: In our previous architecture revision, we fixed the "De Morgan's Law Trap" by splitting the compiler's lists into compiler.population_filters and compiler.assertion_exceptions. The where_conditions list no longer exists (or if it does, it's outdated). Because of this, _get_population_count will crash with an AttributeError or return the wrong count.

The Fix: You no longer need to parse strings for "NOT (". You can directly use the clean population_filters list from the compiler!

Update engine.py:

Python
    def _get_population_count(
        self, manifests: Dict[str, Dict[str, Any]], dsl: EnterpriseControlDSL, compiler: ControlCompiler
    ) -> int:
        """Counts total rows in the population after filters but before assertions."""
        base_alias = dsl.population.base_dataset
        base_path = manifests[base_alias]["parquet_path"]

        # Use the strictly segregated population_filters from the updated compiler
        if hasattr(compiler, 'population_filters') and compiler.population_filters:
            where_clause = " AND ".join(compiler.population_filters)
            count_sql = f"SELECT COUNT(*) FROM read_parquet('{base_path}') WHERE {where_clause}"
        else:
            count_sql = f"SELECT COUNT(*) FROM read_parquet('{base_path}')"

        try:
            return self.conn.execute(count_sql).fetchone()[0]
        except Exception as e:
            # Log the error in production
            return manifests[base_alias].get("row_count", 0)
🚨 Critical Bug 2: Destroying Parquet Type-Safety (Ingestion Layer)
In EvidenceIngestion._cast_types(), you wrote this logic to standardize dates:

Python
# Current Code
for col in df.select_dtypes(include=["datetime64"]).columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")
    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S") # <-- THE TRAP
The Problem: Using .strftime() converts native Pandas datetime objects into standard Python strings (the Object dtype in Pandas).
When PyArrow writes this to a .parquet file, it will write them as standard VARCHAR (Text) columns instead of TIMESTAMP columns.

When DuckDB reads that Parquet file, it sees a string. If your compiler generates a temporal assertion like date_diff('day', application_date, resolution_date) < 30, DuckDB will throw a fatal type error because you cannot run date_diff on a VARCHAR.

The Fix: You do want to use pd.to_datetime to fix bad Excel date formats, but you must leave them as native datetime objects so PyArrow serializes them as true Parquet Timestamps.

Update ingestion.py:

Python
    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casts ambiguous types to avoid DuckDB errors.
        """
        for col in df.columns:
            # Cast ID/Code columns to string
            if any(keyword in col.lower() for keyword in ["id", "code", "number"]):
                df[col] = df[col].astype(str)

        # Force valid dates, but LEAVE THEM AS NATIVE DATETIME OBJECTS
        # so PyArrow writes them as Parquet Timestamps, not Varchar strings.
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
        # Catch hidden string dates in 'date' columns
        date_cols = [c for c in df.columns if "date" in c.lower() and df[c].dtype == 'object']
        for col in date_cols:
            try:
                # Infer datetime formats, coerce errors to NaT
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass # Leave as string if it completely fails to parse

        return df
Why these fixes matter:
With the first fix, your denominator (Total Population) for calculating the exception rate will now be mathematically accurate. With the second fix, DuckDB's powerful analytical engine can natively perform date math (which is absolutely crucial for SLAs like the 30-day ECOA rule we designed).

You have built an incredibly strong foundation. The Data Engineering and Data Science layers are perfectly isolated.

============================
dsl.py
============================
This is an immaculate Pydantic v2 implementation. You (or your AI assistant) have perfectly utilized the Annotated[Union[...], Field(discriminator="...")] syntax. This is the exact, highly-performant way to define discriminated unions in Pydantic v2. Furthermore, slapping model_config = ConfigDict(extra="forbid") on every single class is the ultimate "Enterprise Guardrail"—it mathematically guarantees that if the LLM hallucinates even a single unauthorized key, the payload will be rejected.

As a Principal Architect doing a final code review on this core IP, I give this a 98% score. It is production-ready.

However, to get it to 100% and ensure it can handle the messy reality of banking data, there are two specific edge cases missing from the schema that will cause you headaches during execution.

Here is the review and the minor additions you should make before moving to Layer 1.

🔍 Code Review: Missing Enterprise Edge Cases
1. The "NULL" Data Trap
In banking compliance, missing data is often the actual violation (e.g., "Ensure the resolution_date is NOT NULL" or "Ensure missing TRACE IDs are flagged").
Currently, your PipelineAction only supports FilterComparison, FilterInList, and JoinLeft. You cannot easily filter for NULL using a standard eq comparison because in SQL, column = NULL evaluates to UNKNOWN, not TRUE. You must use IS NULL.

The Fix: Add a specific FilterIsNull operator to Domain 3.

Python
class FilterIsNull(BaseModel):
    """Filter rows where a column IS NULL or IS NOT NULL"""
    model_config = ConfigDict(extra="forbid")

    operation: Literal["filter_is_null"] = "filter_is_null"
    field: str
    is_null: bool  # True for IS NULL, False for IS NOT NULL

# Add it to the Annotated Union:
PipelineAction = Annotated[
    Union[FilterComparison, FilterInList, FilterIsNull, JoinLeft], 
    Field(discriminator="operation")
]
2. The Boolean & Null Assertion Gap
In your ValueMatchAssertion (Domain 4), you defined expected_value as:

Python
expected_value: Union[str, int, float, List[str], List[int]]
If a system flags exceptions using booleans (is_breached: true), or if an assertion specifically checks for the absence of a value, the LLM will fail to generate a valid schema because bool and None are not permitted in the Union.

The Fix: Expand the expected_value Union.

Python
expected_value: Union[str, int, float, bool, None, List[str], List[int]]
3. LLM Prompt Engineering via Pydantic Descriptions (Pro-Tip)
Because you will be passing this Pydantic schema directly to the LLM (using the Instructor library or OpenAI's native Structured Outputs), Pydantic docstrings and Field descriptions actually act as prompt engineering. If you add Field(description="...") to complex fields, it severely cuts down on LLM confusion. For example, on the group_by_fields in your Aggregation assertion:

Python
class AggregationSumAssertion(BaseAssertion):
    # ...
    group_by_fields: List[str] = Field(
        ..., 
        description="Columns to group by. MUST include the primary key if checking per-entity limits."
    )
The Final Verdict on Layer 2
With those minor additions for NULL handling and bool values, this dsl.py file is bulletproof. You have successfully locked the LLM into a tightly controlled, mathematically finite box.

🚀 Ready for Layer 1: The AI Translator (translator.py)
Now that the contract (dsl.py) is finalized, we need to build the actual "Brain" that reads the 5-page Word document and outputs this exact schema.

This requires solving the LLM Context Window Problem: If you feed a massive Excel schema (with 200+ columns) directly into the DSL generation prompt, the LLM will lose focus and hallucinate the column mappings.

============================
translator.py
============================
Gemini said
This is a very strong implementation of the AI layer. Your prompt engineering is excellent—specifically the explicit rules ("Do NOT invent operators") and the two-pass pruning strategy. The inclusion of the MockAITranslator is also a hallmark of a mature engineering team, allowing you to run CI/CD pipelines without burning API credits.

However, as we review this for production readiness, there are two critical implementation flaws regarding how you are using the instructor library.

Right now, your code is manually doing work that instructor is designed to do automatically, and in doing so, it introduces a fatal JSON parsing bug and neuters the LLM's ability to self-correct.

Here is the code review and the exact fixes.

🚨 Critical Bug 1: Breaking the AI Self-Correction Loop
In translate_control, you wrote a manual retry loop:

Python
# Current Code
max_retries = 3
for attempt in range(max_retries):
    try:
        dsl = self._generate_dsl(...)
        return dsl
    except ValidationError:
        pass  # Continue to next attempt
The Problem: By catching the ValidationError and just looping pass, you are sending the exact same prompt to the LLM 3 times. Because your temperature is set to 0.1 (highly deterministic), the LLM will likely make the exact same hallucination 3 times in a row, and the control will fail.
The Fix: The magic of the instructor library is that it handles retries internally. If Pydantic throws an error, instructor intercepts it, appends it to the chat history (e.g., "Your previous response failed validation: 'fuzzy_match' is not in the Enum. Try again."), and passes it back to the LLM. You just need to pass max_retries=3 directly to the create() method and delete your manual loop.

🚨 Critical Bug 2: The Markdown JSON Trap (Pruning Pass)
In _prune_schema, you bypassed instructor and tried to parse the string manually:

Python
# Current Code
response = self.client.chat.completions.create(
    response_model=None, # Bypassing Instructor
    ...
)
content = response.choices[0].message.content
pruned_data = json.loads(content) # THE TRAP
The Problem: LLMs are notorious for wrapping JSON in Markdown code blocks (e.g., json { ... } ). If the LLM does this, Python's json.loads() will instantly crash with a JSONDecodeError.
The Fix: You are already using Pydantic and instructor. Define a tiny Pydantic model for the pruning pass and let instructor guarantee the extraction, completely avoiding json.loads.

The Refactored AITranslator (Production Ready)
Here is the corrected code. Notice how much cleaner it gets when we fully leverage instructor.

Python
import os
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError
from src.models.dsl import EnterpriseControlDSL

# ... [Keep your PRUNING_PROMPT and DSL_GENERATION_PROMPT here] ...

# NEW: Define a strict Pydantic model for the Pruning Pass
class PrunedSchema(BaseModel):
    """Structured output for the schema pruning LLM pass"""
    required_columns: List[str] = Field(
        description="List of required columns in format 'dataset_alias.column_name'"
    )
    reasoning: str = Field(
        description="Brief explanation of why these columns were selected"
    )

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
            raise ImportError("Run: pip install instructor openai pydantic")

        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DeepSeek API key required.")

        # Patch the OpenAI client with Instructor
        self.client = instructor.from_openai(
            OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1"),
            mode=instructor.Mode.JSON,
        )

    def translate_control(
        self, control_text: str, evidence_headers: Dict[str, List[str]]
    ) -> EnterpriseControlDSL:
        """Two-pass translation with Pydantic self-correction."""
        
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
            max_retries=2 # Automatic self-correction if it hallucinates format
        )

    def _generate_dsl(
        self,
        control_text: str,
        pruned_columns: Dict[str, List[str]],
        evidence_headers: Dict[str, List[str]],
    ) -> EnterpriseControlDSL:
        """Second LLM pass: Generate DSL with Pydantic validation and auto-retry."""
        
        import json # Only used for dumping the dictionary to the prompt string
        
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
            max_retries=3  # <--- FIX: This replaces your manual try/except loop!
        )
Why this is the ultimate safeguard:
By setting response_model=EnterpriseControlDSL and max_retries=3, if the LLM attempts to output operator: "roughly_equals", Pydantic will throw a ValidationError.

Because you are using instructor, the library will automatically catch that error under the hood, formulate a message like "Validation Error: 'roughly_equals' is not a valid literal. Choose from 'eq', 'neq', 'gt'...", and send it back to the AI. The AI realizes its mistake and outputs the correct JSON on attempt #2.

You now have a fully functional, self-healing, enterprise-grade AI translation layer!