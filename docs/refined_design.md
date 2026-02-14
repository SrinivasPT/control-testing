# Enterprise Compliance Control Operating System
## Comprehensive Architecture & Implementation Design

**Document Version:** 1.0  
**Last Updated:** February 14, 2026  
**Classification:** Technical Design Document

---

## Table of Contents

1. [Executive Overview](#1-executive-overview)
2. [Business Context & Problem Statement](#2-business-context--problem-statement)
3. [Solution Architecture](#3-solution-architecture)
4. [Domain Model & DSL Specification](#4-domain-model--dsl-specification)
5. [Core System Components](#5-core-system-components)
6. [Data Flow & Execution Pipeline](#6-data-flow--execution-pipeline)
7. [Control Lifecycle & Attestation Framework](#7-control-lifecycle--attestation-framework)
8. [Implementation Roadmap](#8-implementation-roadmap)
9. [Security & Compliance Considerations](#9-security--compliance-considerations)
10. [Performance & Scalability](#10-performance--scalability)
11. [Testing Strategy](#11-testing-strategy)
12. [Deployment Architecture](#12-deployment-architecture)
13. [Monitoring & Observability](#13-monitoring--observability)
14. [Future Enhancements](#14-future-enhancements)

---

## 1. Executive Overview

### 1.1 Purpose

This document defines the complete technical architecture for an Enterprise Compliance Control Operating System—a bank-grade platform that automates the verification of 1,000+ compliance controls against large-scale evidence datasets (1,000–50,000+ rows). The system eliminates manual Excel cross-referencing while maintaining 100% audit defensibility through cryptographic evidence tracking and deterministic execution.

### 1.2 Core Innovation

**Paradigm Shift:** From "AI generates code" to "AI translates policy into immutable data."

Traditional approaches fail regulatory scrutiny because LLMs directly generate executable code, introducing non-determinism and security risks. This platform uses AI strictly as a **semantic translator**—converting plain-English controls into a rigidly typed Domain Specific Language (DSL). All execution is deterministic, mathematically verifiable, and cryptographically auditable.

### 1.3 Key Differentiators

| Aspect | Traditional Approach | This Platform |
|--------|---------------------|---------------|
| **Control Definition** | Custom Python scripts per control | Declarative DSL (JSON) |
| **AI Role** | Generates executable code | Translates to validated schema |
| **Execution** | In-memory Pandas (RAM crashes) | Disk-streaming DuckDB (scalable) |
| **Audit Trail** | Ad-hoc logging | Cryptographic hashing (SHA-256) |
| **Scalability** | Linear (1 engineer per 10 controls) | Exponential (1,000+ controls configured) |
| **Regulatory Acceptance** | Fails audit (black box AI) | Passes audit (deterministic execution) |

---

## 2. Business Context & Problem Statement

### 2.1 Current State Challenges

**Financial institutions face critical compliance bottlenecks:**

1. **Manual Evidence Review:** Analysts spend 80% of time cross-referencing Excel sheets rather than analyzing risk
2. **Non-Scalable Process:** Each new control requires custom Python scripting (4-8 weeks per control)
3. **Human Error Risk:** Manual validation of 10,000+ row datasets leads to missed exceptions
4. **Audit Findings:** Regulators reject AI-based testing due to lack of reproducibility and lineage
5. **Technical Debt:** Fragmented control scripts across Jupyter notebooks, VBA macros, and legacy systems

### 2.2 Regulatory Requirements

Controls must demonstrate:
- **Repeatability:** Identical results on re-execution
- **Explainability:** Clear logic from policy to exception
- **Immutability:** Evidence cannot be altered post-ingestion
- **Lineage:** Complete audit trail from source to result

### 2.3 Target Personas

| Persona | Primary Goals | System Interaction |
|---------|---------------|-------------------|
| **Control Owner** | Define business rules in plain English | Inputs control text; uploads evidence Excel |
| **Compliance QA** | Validate AI-translated logic before production | Reviews generated DSL in approval UI |
| **Internal Auditor** | Verify testing methodology and evidence integrity | Queries execution history; validates cryptographic hashes |
| **External Auditor** | Attest to control effectiveness | Accesses immutable audit ledger with SQL evidence |
| **Remediation Team** | Fix identified exceptions | Receives auto-routed exception reports (JIRA/ServiceNow) |

### 2.4 Success Metrics

- **Operational:** Reduce control testing time from 4 hours to 4 minutes (98% reduction)
- **Scale:** Support 1,000+ controls without linear engineering growth
- **Quality:** Achieve 99.9% exception detection accuracy vs. manual baseline
- **Compliance:** Zero audit findings related to testing methodology (current: 12-15 per year)

---

## 3. Solution Architecture

### 3.1 Architectural Philosophy

**Core Principle: "Controls are Data, Not Code"**

The system treats compliance controls as immutable data structures rather than executable programs. This architectural decision enables:
- Version-controlled policy management
- Mathematical verification of logic correctness
- Complete separation between AI translation and deterministic execution
- Regulatory confidence through cryptographic auditability

### 3.2 The Five-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: AI Semantic Translator (The "Brain")              │
│  • Schema Pruning (Context Management)                      │
│  • DSL Generation (Structured Output)                       │
│  • Validation & Retry Logic                                 │
└─────────────────────────────────────────────────────────────┘
                          ↓ (Plain English → JSON)
┌─────────────────────────────────────────────────────────────┐
│  Layer 2: Canonical Control DSL (The "Core IP")             │
│  • Pydantic v2 Schema (Discriminated Unions)                │
│  • Type-Safe Operator Enumeration                           │
│  • Versioned Governance Metadata                            │
└─────────────────────────────────────────────────────────────┘
                          ↓ (DSL → SQL)
┌─────────────────────────────────────────────────────────────┐
│  Layer 3: Intelligent SQL Compiler (The "Router")           │
│  • Row-Level Assertion → WHERE Clause                       │
│  • Aggregation Assertion → GROUP BY + HAVING                │
│  • Join Pipeline → CTE Generation                           │
└─────────────────────────────────────────────────────────────┘
                          ↓ (SQL → Execution)
┌─────────────────────────────────────────────────────────────┐
│  Layer 4: Disk-Based Execution Engine (The "Muscle")        │
│  • Excel → Parquet Conversion + SHA-256                     │
│  • DuckDB Disk-Streaming (Zero Memory Bloat)               │
│  • Exception Detection & Sampling                           │
└─────────────────────────────────────────────────────────────┘
                          ↓ (Results → Ledger)
┌─────────────────────────────────────────────────────────────┐
│  Layer 5: Cryptographic Audit Fabric (The "Ledger")         │
│  • Immutable Execution History (PostgreSQL)                 │
│  • Evidence Hash Storage                                     │
│  • DSL Version Lineage                                       │
└─────────────────────────────────────────────────────────────┘
```

### 3.3 Technology Stack

| Layer | Component | Technology | Rationale |
|-------|-----------|------------|-----------|
| **Application Framework** | API & Orchestration | Python 3.10+, FastAPI | Industry standard for data/AI integration; async support |
| **AI Translation** | LLM Integration | OpenAI/Anthropic SDK + Instructor | Structured output enforcement; Pydantic native |
| **DSL Validation** | Schema Enforcement | Pydantic v2 | Discriminated unions prevent hallucination |
| **Data Ingestion** | Excel Processing | Pandas, PyArrow | Multi-sheet handling; fast Parquet conversion |
| **Execution Engine** | SQL Runtime | DuckDB | Disk-streaming; OLAP-optimized; zero-copy reads |
| **Audit Ledger** | Persistent Storage | PostgreSQL 14+ | ACID compliance; cryptographic hash storage |
| **Evidence Storage** | Columnar Files | Parquet (on-disk) | Immutable; compressed; DuckDB native |
| **Orchestration** | Background Jobs | Celery + Redis | Scheduled control execution; async tasks |
| **API Gateway** | Web Interface | FastAPI + Uvicorn | OpenAPI auto-docs; async request handling |

### 3.4 System Context Diagram

```
┌──────────────────┐          ┌─────────────────────────────┐
│  Control Owner   │          │   Compliance QA Reviewer    │
│  (Business User) │          │   (Approves DSL Logic)      │
└────────┬─────────┘          └──────────┬──────────────────┘
         │                               │
         │ 1. Submit Plain-English       │ 3. Approve/Reject
         │    Control + Evidence         │    Generated DSL
         │                               │
         v                               v
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│       Enterprise Compliance Control Operating System         │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  AI Layer    │→ │  DSL Store   │→ │  SQL Compiler    │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  Execution   │→ │  Parquet     │→ │  Audit Ledger    │  │
│  │  Engine      │  │  Evidence    │  │  (PostgreSQL)    │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│                                                               │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                │ 4. Exception Reports
                                v
                  ┌──────────────────────────────┐
                  │  External Systems            │
                  │  • JIRA (Remediation)        │
                  │  • ServiceNow (Ticketing)    │
                  │  • PowerBI (Dashboards)      │
                  └──────────────────────────────┘
```

---

## 4. Domain Model & DSL Specification

### 4.1 DSL Design Principles

The Domain Specific Language is the **core intellectual property** of the system. It must:

1. **Be Deterministic:** No ambiguous operators (e.g., no "approximately equals")
2. **Prevent Hallucination:** Use Pydantic discriminated unions to mathematically guarantee valid operator sets
3. **Support Audit:** Every field must be traceable to a regulatory requirement
4. **Enable Versioning:** Immutable once approved; changes create new versions
5. **Remain Human-Readable:** JSON format for business user validation

### 4.2 DSL Schema Domains

The DSL is organized into five logical domains:

#### Domain 1: Control Governance
Metadata linking the control to regulatory frameworks and ownership.

```python
class ControlGovernance(BaseModel):
    control_id: str                    # Unique identifier (e.g., "SOX-IT-001")
    version: str                       # Semantic versioning (e.g., "1.2.0")
    owner_role: str                    # Business owner (e.g., "Treasury Ops")
    testing_frequency: Literal[
        "Continuous",                  # Real-time streaming
        "Daily",                       # T+1 batch
        "Weekly",                      # Monday 00:00 UTC
        "Quarterly",                   # Q1/Q2/Q3/Q4 end
        "Annual"                       # Fiscal year end
    ]
    regulatory_citations: List[str]    # e.g., ["SOX 404", "Basel III CCR"]
    risk_objective: str                # Plain English purpose
```

#### Domain 2: Ontology Bindings
Maps business terminology to physical evidence columns.

```python
class SemanticMapping(BaseModel):
    business_term: str       # Human language (e.g., "Trade Amount")
    dataset_alias: str       # Evidence file reference (e.g., "trade_log")
    technical_field: str     # Actual column name (e.g., "notional_usd")
    data_type: Literal[
        "string",            # Text (max 255 chars)
        "numeric",           # Decimal/Float
        "timestamp",         # ISO 8601 datetime
        "boolean",           # True/False
        "date"               # ISO 8601 date (no time)
    ]
```

**Purpose:** Eliminates ambiguity when the LLM encounters business terms like "approved" vs. technical columns like `approval_status_flag`.

#### Domain 3: Population Pipeline
Defines how to filter and join evidence datasets to create the "in-scope population."

```python
# Base Action Types (Discriminated Union)
class FilterComparison(BaseModel):
    operation: Literal["filter_comparison"] = "filter_comparison"
    field: str
    operator: Literal["eq", "neq", "gt", "lt", "gte", "lte"]
    value: Union[str, int, float, datetime]

class FilterInList(BaseModel):
    operation: Literal["filter_in_list"] = "filter_in_list"
    field: str
    values: List[Union[str, int, float]]

class JoinLeft(BaseModel):
    operation: Literal["join_left"] = "join_left"
    left_dataset: str
    right_dataset: str
    left_key: str
    right_key: str

# Unified Pipeline Step
PipelineAction = Union[FilterComparison, FilterInList, JoinLeft]

class PopulationPipelineStep(BaseModel):
    step_id: str                                    # "step_001"
    action: PipelineAction = Field(..., discriminator="operation")

# SOX-Compliant Sampling Framework
class SamplingStrategy(BaseModel):
    enabled: bool = False
    method: Literal["random", "stratified", "systematic", "judgmental"]
    sample_size: Optional[int] = None              # Absolute count
    sample_percentage: Optional[float] = None       # e.g., 0.25 for 25%
    stratification_field: Optional[str] = None      # For stratified sampling
    random_seed: Optional[int] = 42                 # Reproducibility
    justification: str = ""                         # Auditor documentation

class PopulationPipeline(BaseModel):
    base_dataset: str                               # Starting evidence file
    steps: List[PopulationPipelineStep]             # Chained transformations
    sampling: Optional[SamplingStrategy] = None     # SOX sampling methodology
```

**Example Population Definition:**
```json
{
  "base_dataset": "trade_log",
  "steps": [
    {
      "step_id": "filter_q3",
      "action": {
        "operation": "filter_comparison",
        "field": "trade_date",
        "operator": "gte",
        "value": "2025-07-01"
      }
    },
    {
      "step_id": "join_managers",
      "action": {
        "operation": "join_left",
        "left_dataset": "trade_log",
        "right_dataset": "hr_roster",
        "left_key": "approver_id",
        "right_key": "employee_id"
      }
    }
  ]
}
```

#### Domain 4: Assertion Taxonomy
The "tests" to be performed on each row or group of rows.

```python
class BaseAssertion(BaseModel):
    assertion_id: str                       # "assert_001"
    description: str                        # Human-readable purpose
    materiality_threshold_percent: float    # e.g., 5.0 = "5% failures OK"

# Row-Level Assertion
class ValueMatchAssertion(BaseAssertion):
    assertion_type: Literal["value_match"] = "value_match"
    field: str
    operator: Literal["eq", "neq", "gt", "lt", "gte", "lte", "in", "not_in"]
    expected_value: Union[str, int, float, List[str], List[int]]

# Temporal Sequence Assertion
class TemporalSequenceAssertion(BaseAssertion):
    assertion_type: Literal["temporal_sequence"] = "temporal_sequence"
    event_chain: List[str]    # e.g., ["trade_booked", "trade_approved"]

# Aggregation Assertion
class AggregationSumAssertion(BaseAssertion):
    assertion_type: Literal["aggregation_sum"] = "aggregation_sum"
    group_by_fields: List[str]        # e.g., ["trader_id", "desk"]
    metric_field: str                 # e.g., "notional_amount"
    operator: Literal["gt", "lt", "eq", "gte", "lte"]
    threshold: float                  # e.g., 10000000.00

# Unified Assertion Type
Assertion = Union[
    ValueMatchAssertion,
    TemporalSequenceAssertion,
    AggregationSumAssertion
]
```

**Example Assertion:**
```json
{
  "assertion_id": "assert_001",
  "assertion_type": "value_match",
  "description": "All trades must have an approval status of 'APPROVED'",
  "field": "approval_status",
  "operator": "eq",
  "expected_value": "APPROVED",
  "materiality_threshold_percent": 0.0
}
```

#### Domain 5: Evidence Requirements
Defines audit retention and exception routing rules.

```python
class EvidenceRequirements(BaseModel):
    retention_years: int = 7                    # SOX default
    reviewer_workflow: Literal[
        "Auto-Close_If_Pass",                   # No human touch if 0 exceptions
        "Requires_Human_Signoff",               # Always need approval
        "Four_Eyes_Review"                      # Two independent reviewers
    ]
    exception_routing_queue: str                # e.g., "JIRA:COMPLIANCE-TRADE"
```

### 4.3 Complete DSL Master Schema

```python
class EnterpriseControlDSL(BaseModel):
    """
    The canonical representation of a compliance control.
    Once approved, this JSON becomes immutable and versioned.
    """
    governance: ControlGovernance
    ontology_bindings: List[SemanticMapping]
    population: PopulationPipeline
    assertions: List[Assertion] = Field(..., discriminator="assertion_type")
    evidence: EvidenceRequirements
    
    class Config:
        # Ensure strict validation
        extra = "forbid"
        validate_assignment = True
```

### 4.4 Example: Complete DSL Instance

```json
{
  "governance": {
    "control_id": "SOX-TRADE-001",
    "version": "1.0.0",
    "owner_role": "Trading Compliance",
    "testing_frequency": "Daily",
    "regulatory_citations": ["SOX 404", "MiFID II"],
    "risk_objective": "Ensure all high-value trades are approved by active managers"
  },
  "ontology_bindings": [
    {
      "business_term": "Trade Amount",
      "dataset_alias": "trade_log",
      "technical_field": "notional_usd",
      "data_type": "numeric"
    },
    {
      "business_term": "Approver Status",
      "dataset_alias": "hr_roster",
      "technical_field": "employment_status",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "trade_log",
    "steps": [
      {
        "step_id": "filter_large_trades",
        "action": {
          "operation": "filter_comparison",
          "field": "notional_usd",
          "operator": "gt",
          "value": 10000
        }
      },
      {
        "step_id": "join_approvers",
        "action": {
          "operation": "join_left",
          "left_dataset": "trade_log",
          "right_dataset": "hr_roster",
          "left_key": "approver_id",
          "right_key": "employee_id"
        }
      }
    ]
  },
  "assertions": [
    {
      "assertion_id": "assert_001",
      "assertion_type": "value_match",
      "description": "Approver must be actively employed",
      "field": "employment_status",
      "operator": "eq",
      "expected_value": "ACTIVE",
      "materiality_threshold_percent": 0.0
    },
    {
      "assertion_id": "assert_002",
      "assertion_type": "value_match",
      "description": "Trade must have approval flag set",
      "field": "approval_flag",
      "operator": "eq",
      "expected_value": "Y",
      "materiality_threshold_percent": 0.0
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "JIRA:COMPLIANCE-TRADE"
  }
}
```

---

## 5. Core System Components

### 5.1 Layer 1: AI Semantic Translator

**Purpose:** Convert plain-English control text into validated DSL JSON.

#### 5.1.1 Schema Pruning Strategy

**Problem:** LLMs have context window limits (8K-128K tokens). Large Excel files may have 100+ columns, causing the LLM to lose focus or hallucinate field names.

**Solution:** Two-pass prompting architecture.

**Pass 1: Column Selection**
```python
PRUNING_PROMPT = """
You are a banking data architect.

TASK: Analyze the control procedure and identify the MINIMUM required columns.

INPUT:
- Control Procedure: {control_text}
- Available Columns: {all_column_names}

OUTPUT (JSON):
{
  "required_columns": ["col1", "col2", "col3"],
  "reasoning": "Brief explanation of why each column is needed"
}

RULES:
- Return 3-7 columns maximum
- Use EXACT column names from the provided list
- Prefer explicit columns over wildcards
"""
```

**Pass 2: DSL Generation**
```python
DSL_GENERATION_PROMPT = """
You are a strict compliance control compiler.

TASK: Translate the control procedure into EnterpriseControlDSL JSON.

INPUT:
- Control Procedure: {control_text}
- Pruned Schema: {selected_columns_with_types}
- Evidence Metadata: {dataset_aliases}

OUTPUT: Valid EnterpriseControlDSL JSON

CRITICAL RULES:
1. Use ONLY the columns from the pruned schema
2. Do NOT invent operators - use only: {allowed_operators}
3. Map business terms to technical fields using ontology_bindings
4. For row-level checks, use ValueMatchAssertion
5. For aggregations (SUM, AVG, COUNT), use AggregationSumAssertion
6. Include materiality thresholds (typically 0.0 for binary checks)

EXAMPLE:
If the control says "Ensure all trades over $10k are approved":
- Create a FilterComparison step: notional_usd > 10000
- Create a ValueMatchAssertion: approval_status == "APPROVED"
"""
```

#### 5.1.2 Implementation

```python
from openai import OpenAI
from pydantic import ValidationError
from src.models.dsl import EnterpriseControlDSL
import instructor

class AITranslator:
    def __init__(self, api_key: str):
        self.client = instructor.from_openai(OpenAI(api_key=api_key))
    
    def translate_control(
        self, 
        control_text: str, 
        evidence_headers: dict[str, list[str]]
    ) -> EnterpriseControlDSL:
        """
        Two-pass translation with automatic retry on validation failure.
        """
        # Pass 1: Prune columns
        pruned_columns = self._prune_schema(control_text, evidence_headers)
        
        # Pass 2: Generate DSL (with Pydantic validation)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                dsl = self.client.chat.completions.create(
                    model="gpt-4o",
                    response_model=EnterpriseControlDSL,
                    messages=[
                        {"role": "system", "content": DSL_GENERATION_PROMPT},
                        {"role": "user", "content": self._build_dsl_prompt(
                            control_text, pruned_columns
                        )}
                    ],
                    max_tokens=4000,
                    temperature=0.1  # Low temperature for determinism
                )
                return dsl
            except ValidationError as e:
                if attempt == max_retries - 1:
                    raise ValueError(f"Failed to generate valid DSL after {max_retries} attempts: {e}")
                # Retry with error feedback
                continue
    
    def _prune_schema(self, control_text: str, headers: dict) -> dict:
        """Extract 3-7 relevant columns using first LLM pass."""
        # Implementation details...
        pass
```

**Key Features:**
- **Instructor Library:** Forces LLM output to match Pydantic schema (auto-retry on validation failure)
- **Low Temperature:** 0.1 setting ensures deterministic output
- **Error Feedback Loop:** If Pydantic validation fails, the error is fed back to the LLM for retry

---

### 5.2 Layer 2: Canonical Control DSL

**Purpose:** Act as the immutable "source of truth" for control logic.

#### 5.2.1 File Location
`src/models/dsl.py`

#### 5.2.2 Implementation

See Section 4.3 for the complete Pydantic schema. Key design patterns:

**Discriminated Unions:**
```python
PipelineAction = Union[FilterComparison, FilterInList, JoinLeft]

class PopulationPipelineStep(BaseModel):
    step_id: str
    action: PipelineAction = Field(..., discriminator="operation")
```

**Why Discriminated Unions Matter:**
- Pydantic uses the `operation` field to determine which subclass to instantiate
- If the LLM outputs `"operation": "fuzzy_match"`, Pydantic raises a validation error
- This **mathematically prevents hallucinated operators**

**Type Safety Example:**
```python
# This will PASS validation
{
  "operation": "filter_comparison",
  "field": "status",
  "operator": "eq",
  "value": "ACTIVE"
}

# This will FAIL validation (invalid operator)
{
  "operation": "filter_comparison",
  "field": "status",
  "operator": "roughly_equals",  # ❌ Not in enum
  "value": "ACTIVE"
}
```

---

### 5.3 Layer 3: Intelligent SQL Compiler

**Purpose:** Translate DSL JSON into optimized DuckDB SQL.

#### 5.3.1 File Location
`src/compiler/sql_compiler.py`

#### 5.3.2 Compilation Strategy

**Key Insight:** Different assertion types require different SQL constructs.

| Assertion Type | SQL Pattern |
|----------------|-------------|
| `value_match` | `WHERE field = value` |
| `temporal_sequence` | `WHERE event1_ts < event2_ts` |
| `aggregation_sum` | `GROUP BY ... HAVING SUM(field) > threshold` |

#### 5.3.3 Implementation

```python
from src.models.dsl import EnterpriseControlDSL, ValueMatchAssertion, AggregationSumAssertion

class ControlCompiler:
    def __init__(self, dsl: EnterpriseControlDSL):
        self.dsl = dsl
        self.where_conditions: list[str] = []
        self.having_conditions: list[str] = []
        self.group_by_fields: list[str] = []
        self.cte_fragments: list[str] = []
    
    def compile_to_sql(self, parquet_manifests: dict[str, dict]) -> str:
        """
        Generates SQL that returns EXCEPTIONS (rows that FAIL the control).
        
        Args:
            parquet_manifests: Maps dataset_alias to {'parquet_path': str, 'sha256_hash': str}
        
        Returns:
            DuckDB SQL query string
        """
        # Step 1: Build base CTE from population pipeline
        self._build_population_cte(parquet_manifests)
        
        # Step 2: Route assertions to WHERE or HAVING clauses
        self._compile_assertions()
        
        # Step 3: Construct final query
        return self._construct_query()
    
    def _build_population_cte(self, manifests: dict) -> None:
        """Builds CTE with filters and joins."""
        base_alias = self.dsl.population.base_dataset
        base_path = manifests[base_alias]['parquet_path']
        
        # Start with base dataset
        current_cte = f"base AS (SELECT * FROM read_parquet('{base_path}'))"
        self.cte_fragments.append(current_cte)
        
        # Track the previous CTE alias for proper chaining
        previous_alias = "base"
        
        # Apply pipeline steps
        for step in self.dsl.population.steps:
            action = step.action
            
            if action.operation == "filter_comparison":
                cond = self._compile_filter(action)
                self.where_conditions.append(cond)
            
            elif action.operation == "join_left":
                right_path = manifests[action.right_dataset]['parquet_path']
                # CRITICAL FIX: Reference previous step, not always 'base'
                join_cte = f"""
                {step.step_id} AS (
                    SELECT * FROM {previous_alias}
                    LEFT JOIN read_parquet('{right_path}') AS right_tbl
                    ON {previous_alias}.{action.left_key} = right_tbl.{action.right_key}
                )
                """
                self.cte_fragments.append(join_cte)
                # Update the pointer to the current step for next iteration
                previous_alias = step.step_id
    
    def _compile_assertions(self) -> None:
        """Routes assertions to appropriate SQL clauses."""
        for assertion in self.dsl.assertions:
            if isinstance(assertion, ValueMatchAssertion):
                # Row-level assertion → WHERE clause
                cond = self._compile_value_match(assertion)
                # Wrap in NOT to find exceptions
                self.where_conditions.append(f"NOT ({cond})")
            
            elif isinstance(assertion, AggregationSumAssertion):
                # Aggregation assertion → HAVING clause
                cond = self._compile_aggregation(assertion)
                self.having_conditions.append(f"NOT ({cond})")
                self.group_by_fields.extend(assertion.group_by_fields)
    
    def _compile_value_match(self, assertion: ValueMatchAssertion) -> str:
        """Translates ValueMatchAssertion to SQL condition."""
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
            "not_in": "NOT IN"
        }
        
        sql_op = op_map[operator]
        
        # Handle list values for IN operator
        if operator in ["in", "not_in"]:
            if isinstance(value, list):
                values_str = ", ".join([self._quote_value(v) for v in value])
                return f"{field} {sql_op} ({values_str})"
        
        # Handle scalar values
        return f"{field} {sql_op} {self._quote_value(value)}"
    
    def _compile_aggregation(self, assertion: AggregationSumAssertion) -> str:
        """Translates AggregationSumAssertion to SQL HAVING condition."""
        metric = assertion.metric_field
        operator = assertion.operator
        threshold = assertion.threshold
        
        op_map = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}
        sql_op = op_map[operator]
        
        return f"SUM({metric}) {sql_op} {threshold}"
    
    def _construct_query(self) -> str:
        """Assembles final SQL query."""
        # Build CTE chain
        cte_sql = "WITH " + ",\n".join(self.cte_fragments)
        
        # Build WHERE clause
        where_clause = " AND ".join(self.where_conditions) if self.where_conditions else "1=1"
        
        # Build SELECT
        if self.having_conditions:
            # Aggregation query
            group_fields = ", ".join(set(self.group_by_fields))
            having_clause = " AND ".join(self.having_conditions)
            select_sql = f"""
            SELECT {group_fields}, 
                   COUNT(*) as exception_count,
                   SUM({self.dsl.assertions[0].metric_field}) as total_amount
            FROM base
            WHERE {where_clause}
            GROUP BY {group_fields}
            HAVING {having_clause}
            """
        else:
            # Row-level query
            select_sql = f"""
            SELECT *
            FROM base
            WHERE {where_clause}
            """
        
        return f"{cte_sql}\n{select_sql}"
    
    @staticmethod
    def _quote_value(value) -> str:
        """Safely quotes SQL values with proper escaping."""
        if isinstance(value, str):
            # Escape single quotes (O'Connor -> O''Connor)
            safe_val = value.replace("'", "''")
            return f"'{safe_val}'"
        return str(value)
```

**Example Compiled SQL:**
```sql
WITH base AS (
    SELECT * FROM read_parquet('/data/trade_log.parquet')
),
join_approvers AS (
    SELECT * FROM base
    LEFT JOIN read_parquet('/data/hr_roster.parquet') AS right_tbl
    ON base.approver_id = right_tbl.employee_id
)
SELECT *
FROM base
WHERE NOT (employment_status = 'ACTIVE')
   OR NOT (approval_flag = 'Y')
```

---

### 5.4 Layer 4: Disk-Based Execution Engine

**Purpose:** Ingest Excel evidence and execute compiled SQL without consuming RAM.

#### 5.4.1 Evidence Ingestion Module

**File:** `src/execution/ingestion.py`

```python
import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime

class EvidenceIngestion:
    """
    Converts Excel files to Parquet and generates cryptographic hashes.
    """
    def __init__(self, storage_dir: str):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def ingest_excel_to_parquet(
        self, 
        excel_path: str, 
        dataset_prefix: str,
        source_system: str = "UNKNOWN",
        extraction_timestamp: Optional[datetime] = None
    ) -> list[dict]:
        """
        Loads Excel, sanitizes columns, saves to Parquet, returns manifest.
        
        Args:
            source_system: Name of the source system (e.g., "SAP_FI")
            extraction_timestamp: When data was extracted from source
        
        Returns:
            List of manifests with parquet_path, sha256_hash, source metadata
        """
        path = Path(excel_path)
        sheets = pd.read_excel(path, sheet_name=None, engine='openpyxl')
        manifests = []
        
        for sheet_name, df in sheets.items():
            # 1. Sanitize column names
            df.columns = [
                str(c).strip().replace(' ', '_').lower() 
                for c in df.columns
            ]
            
            # 2. Type casting (prevent DuckDB schema inference errors)
            df = self._cast_types(df)
            
            # 3. Save to Parquet
            out_path = self.storage_dir / f"{dataset_prefix}_{sheet_name}.parquet"
            df.to_parquet(out_path, index=False, engine='pyarrow')
            
            # 4. Generate SHA-256 hash
            file_hash = self._hash_file(out_path)
            
            manifests.append({
                "dataset_alias": f"{dataset_prefix}_{sheet_name}",
                "parquet_path": str(out_path),
                "sha256_hash": file_hash,
                "row_count": len(df),
                "column_count": len(df.columns),
                "source_system": source_system,
                "extraction_timestamp": extraction_timestamp.isoformat() if extraction_timestamp else None,
                "schema_version": "1.0",  # Can be derived from column structure
                "ingested_at": datetime.now().isoformat()
            })
        
        return manifests
    
    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casts ambiguous types to avoid DuckDB errors.
        E.g., ID columns that Pandas infers as int64 should be strings.
        """
        for col in df.columns:
            if 'id' in col.lower() or 'code' in col.lower():
                df[col] = df[col].astype(str)
        
        # Convert datetime columns
        for col in df.select_dtypes(include=['datetime64']).columns:
            df[col] = pd.to_datetime(df[col])
        
        return df
    
    @staticmethod
    def _hash_file(filepath: Path) -> str:
        """Generates SHA-256 hash of file contents."""
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
```

**Key Features:**
- **Column Sanitization:** Converts spaces to underscores (Excel: "Trade ID" → Parquet: "trade_id")
- **Type Safety:** Casts ID columns to strings to prevent Pandas integer inference issues
- **Cryptographic Hashing:** SHA-256 of physical Parquet file (not DataFrame) for audit proof

#### 5.4.2 Execution Engine Module

**File:** `src/execution/engine.py`

```python
import duckdb
from src.models.dsl import EnterpriseControlDSL
from src.compiler.sql_compiler import ControlCompiler

class ExecutionEngine:
    """
    Executes compiled SQL against Parquet files using DuckDB.
    """
    def __init__(self, db_path: str = ':memory:'):
        self.conn = duckdb.connect(db_path)
        # Enable Parquet extensions
        self.conn.execute("INSTALL parquet")
        self.conn.execute("LOAD parquet")
    
    def execute_control(
        self, 
        dsl: EnterpriseControlDSL, 
        manifests: dict[str, dict]
    ) -> dict:
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
            
            # Calculate materiality
            total_population = self._get_population_count(manifests, dsl)
            exception_rate = (exception_count / total_population * 100) if total_population > 0 else 0
            
            # Determine verdict
            max_threshold = max(
                [a.materiality_threshold_percent for a in dsl.assertions]
            )
            verdict = "PASS" if exception_rate <= max_threshold else "FAIL"
            
            return {
                "control_id": dsl.governance.control_id,
                "verdict": verdict,
                "exception_count": exception_count,
                "total_population": total_population,
                "exception_rate_percent": round(exception_rate, 2),
                "execution_query": sql,
                "evidence_hashes": {
                    alias: meta['sha256_hash'] 
                    for alias, meta in manifests.items()
                },
                "exceptions_sample": result.head(100).to_dict(orient="records"),
                "executed_at": datetime.now().isoformat()
            }
        
        except Exception as e:
            return {
                "control_id": dsl.governance.control_id,
                "verdict": "ERROR",
                "error_message": str(e),
                "execution_query": sql,
                "executed_at": datetime.now().isoformat()
            }
    
    def _get_population_count(self, manifests: dict, dsl: EnterpriseControlDSL) -> int:
        """Counts total rows in the base dataset after filters."""
        base_alias = dsl.population.base_dataset
        base_path = manifests[base_alias]['parquet_path']
        
        count_sql = f"SELECT COUNT(*) FROM read_parquet('{base_path}')"
        return self.conn.execute(count_sql).fetchone()[0]
```

**Key Features:**
- **Zero-Copy Reads:** DuckDB's `read_parquet()` streams from disk without loading into memory
- **Materiality Calculation:** Automatically determines if exception rate exceeds threshold
- **Error Handling:** Returns structured error reports instead of crashing

---

### 5.5 Layer 5: Cryptographic Audit Fabric

**Purpose:** Store immutable execution history for regulatory audit.

#### 5.5.1 Database Schema

**File:** `src/storage/audit_fabric.py`

```sql
-- PostgreSQL Schema

CREATE TABLE controls (
    control_id VARCHAR(255) PRIMARY KEY,
    dsl_json JSONB NOT NULL,
    version VARCHAR(50) NOT NULL,
    owner_role VARCHAR(255),
    approved_by VARCHAR(255),
    approved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_control_version UNIQUE (control_id, version)
);

CREATE TABLE evidence_manifests (
    manifest_id SERIAL PRIMARY KEY,
    dataset_alias VARCHAR(255) NOT NULL,
    parquet_path TEXT NOT NULL,
    sha256_hash CHAR(64) NOT NULL,
    row_count INTEGER,
    column_count INTEGER,
    source_system VARCHAR(255),                  -- e.g., "SAP_FI", "Bloomberg"
    extraction_timestamp TIMESTAMP,              -- When data was extracted from source
    extraction_query_hash CHAR(64),              -- SHA-256 of the extraction SQL/API call
    schema_version VARCHAR(50),                  -- Evidence schema version
    ingested_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE executions (
    execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    control_id VARCHAR(255) REFERENCES controls(control_id),
    verdict VARCHAR(20) NOT NULL CHECK (verdict IN ('PASS', 'FAIL', 'ERROR')),
    exception_count INTEGER,
    total_population INTEGER,
    exception_rate_percent DECIMAL(5,2),
    compiled_sql TEXT NOT NULL,
    evidence_hashes JSONB NOT NULL,  -- Maps dataset_alias -> sha256
    exceptions_sample JSONB,          -- First 100 exception rows
    error_message TEXT,
    executed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_executions_control ON executions(control_id);
CREATE INDEX idx_executions_verdict ON executions(verdict);
CREATE INDEX idx_executions_date ON executions(executed_at);

-- Audit Query: Verify Evidence Integrity
CREATE VIEW audit_evidence_lineage AS
SELECT 
    e.execution_id,
    e.control_id,
    e.verdict,
    e.executed_at,
    m.dataset_alias,
    m.sha256_hash AS stored_hash,
    e.evidence_hashes->m.dataset_alias AS execution_hash,
    CASE 
        WHEN m.sha256_hash = e.evidence_hashes->>m.dataset_alias 
        THEN 'VALID'
        ELSE 'TAMPERED'
    END AS integrity_status
FROM executions e
JOIN evidence_manifests m ON e.evidence_hashes ? m.dataset_alias;
```

#### 5.5.2 Python ORM Layer

```python
from sqlalchemy import create_engine, Column, String, Integer, JSON, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json

Base = declarative_base()

class Control(Base):
    __tablename__ = 'controls'
    
    control_id = Column(String(255), primary_key=True)
    dsl_json = Column(JSON, nullable=False)
    version = Column(String(50), nullable=False)
    owner_role = Column(String(255))
    approved_by = Column(String(255))
    approved_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, default=datetime.now)

class Execution(Base):
    __tablename__ = 'executions'
    
    execution_id = Column(String(36), primary_key=True)
    control_id = Column(String(255), nullable=False)
    verdict = Column(String(20), nullable=False)
    exception_count = Column(Integer)
    total_population = Column(Integer)
    compiled_sql = Column(String, nullable=False)
    evidence_hashes = Column(JSON, nullable=False)
    exceptions_sample = Column(JSON)
    executed_at = Column(TIMESTAMP, nullable=False)

class AuditFabric:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def save_control(self, dsl: dict, approved_by: str) -> None:
        """Persists approved DSL to immutable store."""
        session = self.Session()
        try:
            control = Control(
                control_id=dsl['governance']['control_id'],
                dsl_json=dsl,
                version=dsl['governance']['version'],
                owner_role=dsl['governance']['owner_role'],
                approved_by=approved_by,
                approved_at=datetime.now()
            )
            session.add(control)
            session.commit()
        finally:
            session.close()
    
    def save_execution(self, report: dict) -> None:
        """Logs execution result for audit trail."""
        session = self.Session()
        try:
            execution = Execution(
                execution_id=report['execution_id'],
                control_id=report['control_id'],
                verdict=report['verdict'],
                exception_count=report.get('exception_count'),
                total_population=report.get('total_population'),
                compiled_sql=report['execution_query'],
                evidence_hashes=report['evidence_hashes'],
                exceptions_sample=report.get('exceptions_sample'),
                executed_at=datetime.fromisoformat(report['executed_at'])
            )
            session.add(execution)
            session.commit()
        finally:
            session.close()
```

---

## 6. Data Flow & Execution Pipeline

### 6.1 End-to-End Execution Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 1: CONTROL ONBOARDING                                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. User Input                                                         │
│     • Plain-English control text                                      │
│     • Upload Excel evidence files                                     │
│                                                                        │
│  2. AI Translation (Layer 1)                                          │
│     ├─> Pass 1: Schema Pruning (LLM identifies 3-7 columns)          │
│     └─> Pass 2: DSL Generation (LLM outputs JSON)                    │
│                                                                        │
│  3. Validation                                                         │
│     • Pydantic v2 validates against EnterpriseControlDSL schema      │
│     • Auto-retry on validation errors (max 3 attempts)                │
│                                                                        │
│  4. Human Review                                                       │
│     • Compliance QA reviews generated DSL in UI                       │
│     • Approves or rejects with feedback                               │
│                                                                        │
│  5. Persistence (Layer 5)                                             │
│     • Approved DSL saved to PostgreSQL `controls` table              │
│     • Version incremented on changes                                  │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 2: SCHEDULED EXECUTION                                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. Evidence Ingestion (Layer 4)                                      │
│     • Excel → Parquet conversion                                      │
│     • SHA-256 hash generation                                         │
│     • Manifest stored in `evidence_manifests` table                   │
│                                                                        │
│  2. DSL Retrieval                                                      │
│     • Load approved DSL from `controls` table                         │
│                                                                        │
│  3. SQL Compilation (Layer 3)                                         │
│     • Parse DSL assertions                                            │
│     • Route row-level assertions → WHERE clauses                      │
│     • Route aggregations → HAVING clauses                             │
│     • Generate CTE with joins/filters                                 │
│                                                                        │
│  4. Execution (Layer 4)                                               │
│     • DuckDB executes compiled SQL                                    │
│     • Streams Parquet from disk (zero RAM overhead)                   │
│     • Returns exception rows                                          │
│                                                                        │
│  5. Verdict Calculation                                               │
│     • exception_rate = (exceptions / total_population) * 100          │
│     • verdict = "PASS" if rate ≤ threshold else "FAIL"               │
│                                                                        │
│  6. Audit Logging (Layer 5)                                           │
│     • Execution record saved to `executions` table                    │
│     • Includes SQL, evidence hashes, exception sample                 │
│                                                                        │
│  7. Exception Routing                                                  │
│     • If verdict = "FAIL":                                            │
│       └─> Create JIRA ticket with exception sample                    │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  PHASE 3: AUDIT VERIFICATION                                           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. Auditor Access                                                     │
│     • Query `audit_evidence_lineage` view                             │
│     • Verify SHA-256 hashes match physical Parquet files             │
│                                                                        │
│  2. Reproducibility Test                                               │
│     • Re-run execution with same DSL + evidence                       │
│     • Confirm identical exception count                               │
│                                                                        │
│  3. Logic Review                                                       │
│     • Inspect DSL JSON (human-readable)                               │
│     • Validate compiled SQL matches DSL intent                        │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

### 6.2 API Endpoints

```python
from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import BaseModel
from src.ai.translator import AITranslator
from src.execution.ingestion import EvidenceIngestion
from src.execution.engine import ExecutionEngine
from src.storage.audit_fabric import AuditFabric

app = FastAPI(title="Compliance Control Operating System")

# Initialize components
translator = AITranslator(api_key=os.getenv("OPENAI_API_KEY"))
ingestion = EvidenceIngestion(storage_dir="/data/parquet")
engine = ExecutionEngine()
audit = AuditFabric(connection_string=os.getenv("DATABASE_URL"))

@app.post("/controls/translate")
async def translate_control(
    control_text: str,
    evidence_files: list[UploadFile]
):
    """Phase 1: Translate plain-English control to DSL."""
    # Extract headers from Excel files
    headers = {}
    for file in evidence_files:
        df = pd.read_excel(file.file, nrows=0)
        headers[file.filename] = list(df.columns)
    
    # AI translation
    dsl = translator.translate_control(control_text, headers)
    
    return {
        "dsl": dsl.model_dump(),
        "status": "pending_approval"
    }

@app.post("/controls/approve")
async def approve_control(control_id: str, approved_by: str):
    """Phase 1: Lock approved DSL."""
    # Retrieve pending DSL from cache/DB
    dsl = get_pending_dsl(control_id)
    
    # Save to immutable store
    audit.save_control(dsl, approved_by)
    
    return {"status": "approved", "control_id": control_id}

@app.post("/controls/{control_id}/execute")
async def execute_control(
    control_id: str,
    evidence_files: list[UploadFile]
):
    """Phase 2: Execute control against evidence."""
    # Ingest evidence
    manifests = {}
    for file in evidence_files:
        manifest_list = ingestion.ingest_excel_to_parquet(
            file.file,
            dataset_prefix=file.filename.split('.')[0]
        )
        for manifest in manifest_list:
            manifests[manifest['dataset_alias']] = manifest
    
    # Retrieve approved DSL
    dsl = audit.get_control(control_id)
    
    # Execute
    report = engine.execute_control(dsl, manifests)
    
    # Log execution
    audit.save_execution(report)
    
    # Route exceptions if FAIL
    if report['verdict'] == 'FAIL':
        route_to_jira(report)
    
    return report

@app.get("/audit/executions/{control_id}")
async def get_execution_history(control_id: str):
    """Phase 3: Audit trail retrieval."""
    return audit.get_execution_history(control_id)
```

---

## 7. Control Lifecycle & Attestation Framework

### 7.1 Overview

**Critical Gap Identified:** The core platform (Sections 1-6) provides a sophisticated **control testing engine**. However, SOX 404 and internal audit programs require a complete **Control Lifecycle Management System** that tracks:

- **Design Effectiveness:** Is the control designed correctly?
- **Operating Effectiveness:** Does the control operate as designed?
- **Attestation Workflow:** Who certified the control worked?
- **Remediation Closure:** Were exceptions fixed?

This section outlines the **Phase 2 architecture** that elevates the platform from "automated testing" to "enterprise control governance."

### 7.2 Control Effectiveness Model

#### 7.2.1 Design Effectiveness vs Operating Effectiveness

```python
class ControlEffectivenessType(Enum):
    DESIGN = "design"              # Control logic is properly designed
    OPERATING = "operating"        # Control executes as designed
    BOTH = "both"                  # Full SOX certification

class ControlTestingPhase(BaseModel):
    phase: ControlEffectivenessType
    test_type: Literal["walkthrough", "automated", "inquiry", "observation"]
    required_evidence: List[str]
    testing_frequency: str
```

**Design Effectiveness Testing:**
- Performed once when control is onboarded
- Evidence: Control documentation, process flowcharts, DSL review
- Certifier: Control owner + Independent reviewer

**Operating Effectiveness Testing:**
- Performed continuously (daily/quarterly)
- Evidence: Automated execution results from this platform
- Certifier: Control owner + Auditor reliance

#### 7.2.2 Extended Control Governance Schema

```python
class ControlGovernanceExtended(ControlGovernance):
    # Existing fields...
    
    # Lifecycle fields
    design_effectiveness_status: Literal["Designed", "Needs_Revision", "Not_Designed"]
    design_effectiveness_tested_date: Optional[datetime]
    design_effectiveness_tester: Optional[str]
    
    operating_effectiveness_status: Literal["Effective", "Deficient", "Not_Tested"]
    operating_effectiveness_period_start: Optional[datetime]
    operating_effectiveness_period_end: Optional[datetime]
    
    # Attestation fields
    last_certification_date: Optional[datetime]
    certifying_officer: Optional[str]              # e.g., "CFO", "Head of Ops"
    next_certification_due: Optional[datetime]
    
    # Remediation tracking
    open_exceptions_count: int = 0
    oldest_open_exception_date: Optional[datetime]
    remediation_sla_days: int = 30                 # Days to close exceptions
```

### 7.3 Attestation & Certification Workflow

#### 7.3.1 Quarterly SOX Certification Process

```
┌────────────────────────────────────────────────────────────┐
│  QUARTERLY SOX CONTROL CERTIFICATION WORKFLOW              │
├────────────────────────────────────────────────────────────┤
│                                                              │
│  Week 1: Automated Testing                                  │
│  • Platform executes all quarterly controls                 │
│  • Generates exception reports                              │
│  • Routes failures to remediation queue                     │
│                                                              │
│  Week 2-3: Remediation                                      │
│  • Control owners investigate exceptions                    │
│  • Provide business justification or corrective action     │
│  • Update JIRA tickets with resolution                     │
│                                                              │
│  Week 4: Management Review                                  │
│  • Controller reviews summary dashboard                     │
│  • Identifies material deficiencies                         │
│  • Escalates to Audit Committee if needed                  │
│                                                              │
│  Week 5: Certification                                      │
│  • CFO/Controller signs attestation                         │
│  • Certification stored in audit_fabric                     │
│  • External auditor receives evidence package              │
│                                                              │
└────────────────────────────────────────────────────────────┘
```

#### 7.3.2 Database Schema Extension

```sql
-- Certification Attestation Table
CREATE TABLE control_certifications (
    certification_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    control_id VARCHAR(255) REFERENCES controls(control_id),
    certification_period_start DATE NOT NULL,
    certification_period_end DATE NOT NULL,
    certifying_officer VARCHAR(255) NOT NULL,
    certification_date TIMESTAMP NOT NULL,
    effectiveness_conclusion Literal["Effective", "Deficient", "Material_Weakness"],
    executive_summary TEXT,
    digital_signature TEXT,                    -- PKI signature for non-repudiation
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Remediation Tracking Table
CREATE TABLE exception_remediations (
    remediation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id UUID REFERENCES executions(execution_id),
    exception_row_hash CHAR(64) NOT NULL,      -- SHA-256 of the exception row
    assigned_to VARCHAR(255),
    status Literal["Open", "In_Progress", "Closed", "Accepted_Risk"],
    root_cause TEXT,
    corrective_action TEXT,
    target_close_date DATE,
    actual_close_date DATE,
    closure_evidence TEXT,                     -- Link to supporting documentation
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Control Testing Evidence Table (for design effectiveness)
CREATE TABLE control_testing_evidence (
    evidence_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    control_id VARCHAR(255) REFERENCES controls(control_id),
    test_type Literal["walkthrough", "inquiry", "observation", "automated"],
    effectiveness_type Literal["design", "operating"],
    test_date DATE NOT NULL,
    tester_name VARCHAR(255) NOT NULL,
    test_outcome Literal["Pass", "Fail", "Needs_Improvement"],
    test_notes TEXT,
    supporting_documents TEXT[],               -- Array of file paths/URLs
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 7.4 Control Failure Classification

#### 7.4.1 Severity Taxonomy

```python
class ControlDeficiencyLevel(Enum):
    NONE = "none"                              # Control passed
    DEFICIENCY = "deficiency"                  # Isolated failure, no material impact
    SIGNIFICANT_DEFICIENCY = "significant"     # Pattern of failures, needs attention
    MATERIAL_WEAKNESS = "material"             # Systemic failure, affects financial statements

class ControlFailureClassification(BaseModel):
    execution_id: str
    deficiency_level: ControlDeficiencyLevel
    quantitative_impact: Optional[Decimal]     # Dollar amount at risk
    likelihood_score: int                      # 1-5 scale
    impact_score: int                          # 1-5 scale
    risk_score: int                            # likelihood * impact
    requires_disclosure: bool                  # SEC 10-K/10-Q disclosure
    classification_date: datetime
    classified_by: str                         # Risk officer
```

**Classification Logic:**
- **Deficiency:** Exception rate > materiality threshold but < 5%
- **Significant Deficiency:** Exception rate > 5% OR monetary impact > $100K
- **Material Weakness:** Exception rate > 10% OR systematic control design flaw

### 7.5 Integration with Core Platform

#### 7.5.1 Extended API Endpoints

```python
@app.post("/controls/{control_id}/certify")
async def certify_control(
    control_id: str,
    certifying_officer: str,
    period_start: date,
    period_end: date,
    effectiveness_conclusion: str
):
    """Phase 2: Management attestation of control effectiveness."""
    # Retrieve all executions in certification period
    executions = audit.get_executions_in_period(control_id, period_start, period_end)
    
    # Calculate aggregate statistics
    pass_rate = sum(1 for e in executions if e['verdict'] == 'PASS') / len(executions)
    
    # Create certification record
    certification = {
        'control_id': control_id,
        'certifying_officer': certifying_officer,
        'certification_period_start': period_start,
        'certification_period_end': period_end,
        'effectiveness_conclusion': effectiveness_conclusion,
        'pass_rate': pass_rate,
        'certification_date': datetime.now()
    }
    
    audit.save_certification(certification)
    return certification

@app.get("/audit/sox-report")
async def generate_sox_report(fiscal_quarter: str):
    """Phase 2: Generate comprehensive SOX 404 report."""
    # Aggregate all certifications for the quarter
    # Identify material weaknesses
    # Generate PDF audit report
    pass
```

### 7.6 Implementation Priority

**This section represents Phase 2 work** to be implemented **after** the core testing engine is deployed and stable.

**Estimated Timeline:** 8-12 weeks post-MVP  
**Dependencies:** 6+ months of operational data from Phase 1  
**Stakeholders:** CFO, Controller, Internal Audit, External Auditors

---

## 8. Implementation Roadmap

### Phase 1: Core Engine (Weeks 1-4)

**Deliverables:**
- [ ] DSL Pydantic models (`src/models/dsl.py`)
- [ ] SQL Compiler (`src/compiler/sql_compiler.py`)
- [ ] DuckDB Execution Engine (`src/execution/engine.py`)
- [ ] PostgreSQL Audit Fabric schema
- [ ] Unit tests for compiler (10+ test cases)

**Acceptance Criteria:**
- Compiler correctly translates 10 sample DSL instances to SQL
- Execution engine processes 50,000-row Parquet file in <5 seconds

### Phase 2: AI Integration (Weeks 5-6)

**Deliverables:**
- [ ] AI Translator with schema pruning (`src/ai/translator.py`)
- [ ] Integration tests with GPT-4o
- [ ] Validation retry logic
- [ ] 20 real-world control translation examples

**Acceptance Criteria:**
- 95% successful translation rate on first attempt
- Zero hallucinated operators in 100-control test set

### Phase 3: Web API (Weeks 7-8)

**Deliverables:**
- [ ] FastAPI application
- [ ] Evidence upload endpoints
- [ ] DSL approval workflow
- [ ] OpenAPI documentation

**Acceptance Criteria:**
- API handles 1GB Excel file upload without timeout
- Approval workflow logs to audit table

### Phase 4: Scheduler & Routing (Weeks 9-10)

**Deliverables:**
- [ ] Celery task scheduler
- [ ] JIRA exception routing integration
- [ ] Email notification system
- [ ] Dashboard API (control status, exception trends)

**Acceptance Criteria:**
- Daily scheduled execution of 100 controls completes in <30 minutes
- Exception tickets auto-create in JIRA with attachments

### Phase 5: Security & Audit (Weeks 11-12)

**Deliverables:**
- [ ] Role-based access control (RBAC)
- [ ] Evidence tamper detection (hash verification)
- [ ] Audit report generator (PDF)
- [ ] Penetration testing

**Acceptance Criteria:**
- Pass OWASP Top 10 security scan
- Generate SOX-compliant audit report for sample control

---

## 8. Security & Compliance Considerations

### 8.1 Data Protection

**PII/PHI Handling:**
- LLM never sees actual evidence data—only column headers
- Evidence stored on-premise (not sent to cloud AI providers)
- Parquet files encrypted at rest (AES-256)

**Access Control:**
- PostgreSQL row-level security (RLS) by business unit
- API authentication via OAuth 2.0 + JWT
- Audit log of all DSL modifications (WORM storage)

### 8.2 Regulatory Compliance

**SOX 404 Requirements:**
- ✅ Immutable audit trail (PostgreSQL append-only tables)
- ✅ Evidence integrity verification (SHA-256 hashing)
- ✅ Human-in-the-loop approval (DSL signoff workflow)
- ✅ Reproducible execution (deterministic SQL)

**Basel III / MiFID II:**
- ✅ Lineage tracking (DSL version → Execution → Evidence hash)
- ✅ Retention policy (7-year evidence storage)
- ✅ Exception reporting (automated remediation queue)

### 8.3 AI Governance

**Explainability:**
- DSL is human-readable JSON (not opaque model weights)
- Compiled SQL can be inspected by auditors
- AI translation is logged with prompt/response pairs

**Bias Mitigation:**
- LLM only translates logic (no decisioning on sensitive attributes)
- Deterministic execution eliminates AI drift

---

## 9. Performance & Scalability

### 9.1 Performance Targets

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Excel Ingestion (10K rows) | <10 seconds | Time from upload to Parquet |
| SQL Compilation | <1 second | DSL to SQL generation |
| Execution (50K rows, 3 assertions) | <5 seconds | DuckDB query time |
| End-to-End (translation + execution) | <60 seconds | API response time |

### 9.2 Scalability Architecture

**Horizontal Scaling:**
- Celery workers on Kubernetes (auto-scale based on queue depth)
- Read replicas for PostgreSQL audit queries
- S3/MinIO for Parquet storage (distributed file system)

**Optimization Strategies:**
- **Parquet Partitioning:** Partition by `execution_date` for faster queries
- **DuckDB Parallelism:** Enable multi-threading for aggregations
- **Incremental Execution:** Only re-test changed records (delta detection)

### 9.3 Load Testing Results (Projected)

**Test Scenario:** 1,000 controls, 100K rows each, daily execution

- **Total Evidence Size:** 100 GB
- **Execution Time:** 45 minutes (parallelized across 10 workers)
- **Peak Memory:** 4 GB per worker (DuckDB disk streaming)
- **Database Growth:** 500 MB/day (compressed execution logs)

---

## 10. Testing Strategy

### 10.1 Unit Tests

**DSL Validation:**
```python
def test_dsl_rejects_invalid_operator():
    invalid_dsl = {
        "operation": "filter_comparison",
        "field": "status",
        "operator": "fuzzy_match",  # Invalid
        "value": "ACTIVE"
    }
    with pytest.raises(ValidationError):
        FilterComparison(**invalid_dsl)
```

**SQL Compilation:**
```python
def test_compiler_generates_where_clause():
    dsl = create_sample_dsl_with_value_match()
    compiler = ControlCompiler(dsl)
    sql = compiler.compile_to_sql(mock_manifests)
    assert "WHERE NOT (status = 'ACTIVE')" in sql
```

### 10.2 Integration Tests

**End-to-End Execution:**
```python
def test_control_execution_pass():
    # Setup: Create test Parquet with compliant data
    df = pd.DataFrame({
        'trade_id': [1, 2, 3],
        'amount': [5000, 15000, 25000],
        'approval_status': ['APPROVED', 'APPROVED', 'APPROVED']
    })
    df.to_parquet('/tmp/trades.parquet')
    
    # Execute control
    dsl = load_sample_dsl("all_trades_approved")
    manifests = {'trade_log': {'parquet_path': '/tmp/trades.parquet'}}
    
    result = engine.execute_control(dsl, manifests)
    
    assert result['verdict'] == 'PASS'
    assert result['exception_count'] == 0
```

### 10.3 AI Translation Tests

**Regression Suite:**
- 50 real-world control texts
- Expected DSL outputs (golden set)
- Automated comparison on each model update

---

## 11. Deployment Architecture

### 11.1 Infrastructure Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  KUBERNETES CLUSTER (On-Premise / AWS EKS)                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  FastAPI     │  │  Celery      │  │  Celery      │      │
│  │  (API Server)│  │  Worker 1    │  │  Worker 2    │      │
│  │  Replicas: 3 │  │  Replicas: 5 │  │  Replicas: 5 │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         ↓                  ↓                  ↓              │
│  ┌──────────────────────────────────────────────────┐      │
│  │  Redis (Message Broker)                          │      │
│  └──────────────────────────────────────────────────┘      │
│         ↓                                                    │
│  ┌──────────────────────────────────────────────────┐      │
│  │  PostgreSQL (Audit Ledger)                       │      │
│  │  • Primary + 2 Read Replicas                     │      │
│  └──────────────────────────────────────────────────┘      │
│         ↓                                                    │
│  ┌──────────────────────────────────────────────────┐      │
│  │  S3 / MinIO (Parquet Evidence Storage)          │      │
│  │  • Versioned bucket                              │      │
│  │  • 7-year retention policy                       │      │
│  └──────────────────────────────────────────────────┘      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### 11.2 Deployment Configuration

**Docker Compose (Local Development):**
```yaml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/compliance
      - REDIS_URL=redis://redis:6379
    depends_on:
      - postgres
      - redis

  worker:
    build: .
    command: celery -A src.tasks worker --loglevel=info
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/compliance
      - REDIS_URL=redis://redis:6379
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:14
    environment:
      - POSTGRES_PASSWORD=changeme
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7

volumes:
  postgres_data:
```

---

## 12. Monitoring & Observability

### 12.1 Key Metrics

**Operational Metrics:**
- Control execution success rate (target: 99.5%)
- Average execution time per control
- Evidence ingestion throughput (MB/sec)
- API latency (p50, p95, p99)

**Business Metrics:**
- Total controls onboarded
- Exception detection rate
- Remediation time (JIRA ticket age)
- Audit findings (target: 0)

### 12.2 Logging Strategy

**Structured Logging:**
```python
import structlog

logger = structlog.get_logger()

logger.info(
    "control_executed",
    control_id="SOX-TRADE-001",
    verdict="FAIL",
    exception_count=12,
    evidence_hash="a3f5b8c2..."
)
```

**Log Aggregation:**
- Ship logs to ELK Stack (Elasticsearch, Logstash, Kibana)
- Create dashboards for control execution trends
- Alert on ERROR-level logs (PagerDuty integration)

### 12.3 Alerting Rules

**Critical Alerts (Immediate Response):**
- Control execution rate drops below 90%
- Evidence tamper detection (hash mismatch)
- AI translation failure rate >10%

**Warning Alerts (Next Business Day):**
- Execution time exceeds 2x baseline
- PostgreSQL disk usage >80%
- Exception count spike (>3 standard deviations)

---

## 14. Future Enhancements

This section documents architectural improvements identified during peer review that should be considered for **post-MVP releases** as the platform scales from 50 to 1,000+ controls.

### 14.1 Schema Drift Detection & Auto-Remediation

**Problem:** Bank Excel files evolve. IT systems may rename columns (e.g., `notional_amount` → `notional_usd`), causing DuckDB to throw "Column Not Found" errors during execution.

**Solution: Pre-Flight Schema Validation**

```python
class SchemaValidator:
    def validate_before_execution(self, dsl: EnterpriseControlDSL, manifests: dict) -> ValidationResult:
        """
        Compares expected columns (from DSL ontology_bindings) against 
        actual columns in Parquet files.
        """
        expected_columns = {binding.technical_field for binding in dsl.ontology_bindings}
        
        for dataset_alias, manifest in manifests.items():
            actual_columns = self._read_parquet_schema(manifest['parquet_path'])
            missing = expected_columns - set(actual_columns)
            
            if missing:
                # Trigger AI to propose updated SemanticMapping
                suggested_mapping = ai_translator.suggest_column_mapping(
                    missing_columns=missing,
                    available_columns=actual_columns,
                    control_context=dsl.governance.risk_objective
                )
                return ValidationResult(
                    status="SCHEMA_DRIFT_DETECTED",
                    missing_columns=list(missing),
                    suggested_remapping=suggested_mapping
                )
        
        return ValidationResult(status="VALID")
```

**Implementation Priority:** Medium (6-9 months post-launch)  
**Trigger:** After 3+ schema drift incidents

### 14.2 Advanced Date Standardization

**Problem:** Pandas misinterprets ambiguous dates (04/05/2025 — April 5th vs May 4th?), causing silent data corruption.

**Solution: Strict ISO-8601 Enforcement**

```python
class DateStandardizer:
    def standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Forces all date/datetime columns to ISO-8601 format before Parquet save.
        """
        date_columns = df.select_dtypes(include=['datetime64', 'object']).columns
        
        for col in date_columns:
            if self._is_date_column(col):
                # Parse with explicit format
                df[col] = pd.to_datetime(
                    df[col], 
                    format='%Y-%m-%d',        # ISO-8601 date
                    errors='coerce'            # NaT for invalid dates
                )
                # Convert to ISO string for DuckDB
                df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        return df
```

**Implementation Priority:** High (3-4 months post-launch)  
**Rationale:** Prevents silent data quality issues in financial reconciliations

### 14.3 Extended Population Operations

**Problem:** Complex banking reconciliations require operations beyond filters and joins:
- **UNION ALL:** Combine 5 trade files before joining to pricing
- **Window Functions:** Calculate running balances
- **Deduplication:** Remove duplicate transactions
- **Temporal Grouping:** Aggregate by fiscal quarter

**Solution: Expand DSL Pipeline Actions**

```python
class UnionAll(BaseModel):
    operation: Literal["union_all"] = "union_all"
    datasets: List[str]                          # Multiple datasets to union

class Deduplicate(BaseModel):
    operation: Literal["deduplicate"] = "deduplicate"
    key_fields: List[str]                        # Unique key for deduplication
    keep: Literal["first", "last"] = "first"

class WindowFunction(BaseModel):
    operation: Literal["window_function"] = "window_function"
    function: Literal["row_number", "rank", "lag", "lead", "sum"]
    partition_by: List[str]
    order_by: List[str]
    output_field: str

# Extended Pipeline Action
PipelineActionExtended = Union[
    FilterComparison, 
    FilterInList, 
    JoinLeft,
    UnionAll,           # NEW
    Deduplicate,        # NEW
    WindowFunction      # NEW
]
```

**Implementation Priority:** Medium-High (4-6 months post-launch)  
**Trigger:** When 10+ controls require these operations

### 14.4 Execution Engine Abstraction Layer

**Problem:** DuckDB works well for 50K-500K rows, but banks eventually need:
- **Apache Spark:** For 10M+ row datasets
- **Snowflake/Databricks:** For governed cloud data lakes
- **Pushdown to Source:** Execute SQL directly in SAP/Oracle

**Solution: Pluggable Execution Engine Interface**

```python
from abc import ABC, abstractmethod

class ExecutionEngine(ABC):
    @abstractmethod
    def execute_sql(self, sql: str) -> pd.DataFrame:
        pass

class DuckDBEngine(ExecutionEngine):
    def execute_sql(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).df()

class SparkEngine(ExecutionEngine):
    def execute_sql(self, sql: str) -> pd.DataFrame:
        return self.spark.sql(sql).toPandas()

class SnowflakeEngine(ExecutionEngine):
    def execute_sql(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.snowflake_conn)

# Compiler generates SQL agnostic of engine
class ControlCompiler:
    def __init__(self, dsl: EnterpriseControlDSL, engine: ExecutionEngine):
        self.dsl = dsl
        self.engine = engine  # Injected dependency
```

**Implementation Priority:** Low-Medium (12+ months post-launch)  
**Trigger:** First dataset > 1M rows or cloud data lake requirement

### 14.5 AI Translation Governance Enhancements

**Problem:** LLM models evolve (GPT-4 → GPT-5), causing translation drift. Regulators require proof that DSL generation is stable.

**Solution: Translation Regression Testing Framework**

```python
class TranslationGovernance:
    def __init__(self):
        self.golden_dataset = []  # 100 control texts + expected DSL
    
    def test_translation_stability(self, model_version: str) -> Report:
        """
        Re-translates all golden controls with new model.
        Flags any DSL changes vs. approved baseline.
        """
        drift_count = 0
        
        for golden_case in self.golden_dataset:
            new_dsl = ai_translator.translate(
                golden_case['control_text'],
                model=model_version
            )
            
            if new_dsl != golden_case['approved_dsl']:
                drift_count += 1
                # Log for review
        
        return Report(
            model_version=model_version,
            test_cases=len(self.golden_dataset),
            drift_cases=drift_count,
            drift_rate=drift_count / len(self.golden_dataset)
        )
```

**Implementation Priority:** Medium (6-9 months post-launch)  
**Components:**
- Model version pinning in DSL metadata
- Golden DSL regression suite (100+ cases)
- Automated drift detection on model upgrades
- Explainability artifacts for audit

### 14.6 Real-Time Streaming Controls

**Problem:** Current design assumes batch execution (daily/weekly). High-risk controls (fraud detection, trading limits) require **real-time streaming**.

**Solution: Kafka + Flink Integration**

```python
class StreamingControlEngine:
    def __init__(self, kafka_broker: str):
        self.consumer = KafkaConsumer(kafka_broker)
        self.flink_env = StreamExecutionEnvironment.get_execution_environment()
    
    def deploy_streaming_control(self, dsl: EnterpriseControlDSL):
        """
        Converts DSL assertions to Flink SQL and deploys as streaming job.
        """
        flink_sql = self._compile_to_flink_sql(dsl)
        
        # Create Flink streaming job
        self.flink_env.execute_sql(f"""
            CREATE TABLE trades (
                trade_id STRING,
                amount DOUBLE,
                timestamp TIMESTAMP(3),
                WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'trade-stream',
                'properties.bootstrap.servers' = '{self.kafka_broker}'
            );
            
            CREATE TABLE violations AS
            SELECT * FROM trades
            WHERE {flink_sql};  -- Compiled from DSL
        """)
```

**Implementation Priority:** Low (18+ months post-launch)  
**Use Cases:** Trading limit monitoring, fraud detection, real-time reconciliation

---

## Appendix A: Sample Control Translation

### Input (Plain English)
```
Control ID: SOX-TRADE-001
Procedure: Ensure all trades with a notional amount exceeding $10,000 
executed in Q3 2025 have been approved by an active manager (employment 
status = 'ACTIVE') before settlement.

Evidence Required:
- Trade Log (trade_log.xlsx)
- HR Roster (hr_roster.xlsx)
```

### Output (DSL JSON)
```json
{
  "governance": {
    "control_id": "SOX-TRADE-001",
    "version": "1.0.0",
    "owner_role": "Trading Compliance",
    "testing_frequency": "Quarterly",
    "regulatory_citations": ["SOX 404"],
    "risk_objective": "Prevent unauthorized high-value trades"
  },
  "ontology_bindings": [
    {
      "business_term": "Trade Amount",
      "dataset_alias": "trade_log",
      "technical_field": "notional_amount",
      "data_type": "numeric"
    },
    {
      "business_term": "Manager Status",
      "dataset_alias": "hr_roster",
      "technical_field": "employment_status",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "trade_log",
    "steps": [
      {
        "step_id": "filter_q3",
        "action": {
          "operation": "filter_comparison",
          "field": "trade_date",
          "operator": "gte",
          "value": "2025-07-01"
        }
      },
      {
        "step_id": "filter_large",
        "action": {
          "operation": "filter_comparison",
          "field": "notional_amount",
          "operator": "gt",
          "value": 10000
        }
      },
      {
        "step_id": "join_managers",
        "action": {
          "operation": "join_left",
          "left_dataset": "trade_log",
          "right_dataset": "hr_roster",
          "left_key": "approver_id",
          "right_key": "employee_id"
        }
      }
    ]
  },
  "assertions": [
    {
      "assertion_id": "assert_001",
      "assertion_type": "value_match",
      "description": "Approver must be actively employed",
      "field": "employment_status",
      "operator": "eq",
      "expected_value": "ACTIVE",
      "materiality_threshold_percent": 0.0
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "JIRA:COMPLIANCE-TRADE"
  }
}
```

### Generated SQL
```sql
WITH base AS (
    SELECT * FROM read_parquet('/data/trade_log.parquet')
    WHERE trade_date >= '2025-07-01'
      AND notional_amount > 10000
),
join_managers AS (
    SELECT 
        base.*,
        hr.employment_status
    FROM base
    LEFT JOIN read_parquet('/data/hr_roster.parquet') AS hr
        ON base.approver_id = hr.employee_id
)
SELECT *
FROM join_managers
WHERE NOT (employment_status = 'ACTIVE')
```

---

## Appendix B: Technology Alternatives Considered

| Decision | Selected | Alternatives Considered | Rationale |
|----------|----------|------------------------|-----------|
| **Execution Engine** | DuckDB | Spark, Presto, Pandas | DuckDB: Embedded (no cluster), disk-streaming, 10x faster than Pandas on large files |
| **DSL Validation** | Pydantic v2 | JSON Schema, Protobuf | Pydantic: Native Python integration, discriminated unions, automatic retry logic with Instructor |
| **Evidence Format** | Parquet | CSV, Avro, ORC | Parquet: Columnar (DuckDB optimized), immutable, self-describing schema |
| **Audit Ledger** | PostgreSQL | MongoDB, Cassandra | PostgreSQL: ACID compliance, SQL auditor familiarity, JSONB for DSL storage |
| **AI Provider** | OpenAI/Anthropic | Fine-tuned OSS (Llama) | OpenAI: 95%+ structured output accuracy, no fine-tuning overhead, enterprise SLAs |

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-14 | System Architect | Initial comprehensive design |
| 1.1 | 2026-02-14 | System Architect | **Critical fixes from peer review:**<br/>• Fixed CTE chaining bug in SQL compiler<br/>• Fixed SQL injection vulnerability in value quoting<br/>• Added SOX-compliant sampling framework<br/>• Enhanced evidence manifest with source metadata<br/>• Added Control Lifecycle & Attestation architecture (Section 7)<br/>• Added Future Enhancements roadmap (Section 14) |

---

**End of Document**
