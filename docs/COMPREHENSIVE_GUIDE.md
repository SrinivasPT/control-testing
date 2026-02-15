# Enterprise Compliance Control Operating System - Comprehensive Guide

**Version:** 2.0  
**Last Updated:** February 15, 2026  
**Status:** Production Ready

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Quick Start](#quick-start)
4. [Project Structure](#project-structure)
5. [Orchestrator Usage](#orchestrator-usage)
6. [DSL Reference](#dsl-reference)
7. [AI Translation & Validation](#ai-translation--validation)
8. [Self-Healing System](#self-healing-system)
9. [Audit Trail & Compliance](#audit-trail--compliance)
10. [Development Guide](#development-guide)
11. [Troubleshooting](#troubleshooting)

---

## System Overview

### What Is This?

A Tier-1 bank-grade platform that automates verification of 1,000+ compliance controls against large-scale evidence datasets. The core philosophy: **"Controls are Data, Not Code."**

### Key Capabilities

- ✅ **AI-Powered Translation**: Converts plain English verification procedures to validated DSL
- ✅ **Type-Safe DSL**: Pydantic v2 with discriminated unions prevents AI hallucination
- ✅ **Deterministic Execution**: DuckDB disk-streaming for memory-safe processing of large datasets
- ✅ **Cryptographic Audit Trail**: SHA-256 hashing of evidence with immutable SQLite ledger
- ✅ **SOX Compliant**: Statistical sampling, population counting, and execution history
- ✅ **AI Self-Healing**: Automatic DSL correction when SQL validation fails
- ✅ **Optional LLM Validation**: Pre-flight DSL and SQL review for extra safety

### Design Principles

1. **AI as Translator, Not Executor**: AI only converts English → DSL (using headers, never PII)
2. **Mathematical Determinism**: Execution is 100% deterministic via DuckDB SQL
3. **Zero Trust**: All SQL is validated by DuckDB EXPLAIN before execution
4. **Audit Defensibility**: Every execution cryptographically linked to evidence files

---

## Architecture

### 5-Layer Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 1: AI Semantic Translator (DeepSeek + Schema Pruning)    │
│           ↓ Generates Pydantic DSL                               │
├──────────────────────────────────────────────────────────────────┤
│  Layer 2: Canonical Control DSL (Pydantic v2 + Discriminators)  │
│           ↓ Compiles to SQL                                       │
├──────────────────────────────────────────────────────────────────┤
│  Layer 3: SQL Compiler (CTE Chaining + Value Escaping)          │
│           ↓ Validates with EXPLAIN                               │
├──────────────────────────────────────────────────────────────────┤
│  Layer 4: Execution Engine (DuckDB Disk Streaming + Parquet)    │
│           ↓ Returns exceptions                                    │
├──────────────────────────────────────────────────────────────────┤
│  Layer 5: Audit Fabric (SQLite Ledger + SHA-256 Verification)   │
└──────────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| AI Translation | DeepSeek | Cost-effective, instruction-following, structured output |
| DSL Validation | Pydantic v2 | Type safety, discriminated unions, automatic validation |
| Data Ingestion | Pandas + PyArrow | Excel → Parquet conversion with cryptographic hashing |
| Execution | DuckDB | Disk-streaming SQL engine, no memory limits |
| Audit Ledger | SQLite | ACID compliance, local storage, zero-cost auditing |

### Data Flow

```
┌─────────────────────┐
│ control-information │ (Plain English)
│        .md          │
└──────────┬──────────┘
           │
           ▼
    ┌──────────────┐
    │  AI Translator│ ← Only sees column headers (PII-safe)
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │   DSL JSON   │ (Validated by Pydantic)
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │ SQL Compiler │ ← Generates deterministic SQL
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │ EXPLAIN Check│ ← DuckDB validates SQL (Strict Judge)
    └──────┬───────┘
           │
           ▼ (if PASS)
    ┌──────────────┐              ┌──────────────┐
    │ Excel Files  │──hash(SHA256)│   Parquet    │
    │   (.xlsx)    │──────────────▶│    Files     │
    └──────────────┘              └──────┬───────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ DuckDB Query │
                                  │  (Exceptions)│
                                  └──────┬───────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ Audit Ledger │
                                  │  (SQLite DB) │
                                  └──────────────┘
```

---

## Quick Start

### Installation

```bash
# 1. Clone repository
git clone <repo-url>
cd control-tester

# 2. Install dependencies (Python 3.10+)
pip install -r requirements.txt

# 3. Create .env file
echo "DEEPSEEK_API_KEY=your-api-key-here" > .env
```

### Run Your First Control Test

```bash
# Process all projects in data/input/
python -m src.orchestrator --input data/input

# Or use mock AI for testing (no API key needed)
python -m src.orchestrator --input data/input --mock-ai
```

### Expected Output

```
============================================================
🚀 ENTERPRISE CONTROL ORCHESTRATOR
============================================================
Found 3 project(s) to process

────────────────────────────────────────────────────────────
📁 Processing Project: CTRL-IAM-007
────────────────────────────────────────────────────────────

[1/5] 📄 Reading control-information.md...
   Control ID: CTRL-IAM-007

[2/5] 📊 Scanning for Excel evidence files...
   Found 1 file(s): user_access_log.xlsx

[3/5] 🧠 Checking for existing DSL in audit database...
   ✓ DSL found (version 1.0.0) - reusing cached version

[4/5] 🔄 Ingesting Excel files to Parquet with cryptographic hashing...
   Processing: user_access_log.xlsx...
      ✓ user_access_log_sheet1: 5000 rows, hash: a3f2c1d8e...

[5/6] ✅ DuckDB EXPLAIN Validation (Strict Judge)...
   ✓ SQL validation PASSED - query is correct

[6/6] ⚙️  Executing control via DuckDB SQL engine...
   ✓ Execution complete - results saved to audit database

✅ VERDICT: PASS
   Exceptions: 0/5000 (0.00%)
```

---

## Project Structure

```
control-tester/
├── src/                          # Source code (5 layers)
│   ├── ai/                       # Layer 1: AI Translation & Validation
│   │   ├── translator.py         # DeepSeek-based DSL generator
│   │   └── validator.py          # LLM pre-flight validation
│   ├── models/                   # Layer 2: DSL Definitions
│   │   └── dsl.py                # Pydantic models with discriminators
│   ├── compiler/                 # Layer 3: SQL Compilation
│   │   └── sql_compiler.py       # DSL → SQL transformer
│   ├── execution/                # Layer 4: Execution Engine
│   │   ├── engine.py             # DuckDB query executor
│   │   └── ingestion.py          # Excel → Parquet converter
│   ├── storage/                  # Layer 5: Audit Ledger
│   │   └── audit_fabric.py       # SQLite-based audit trail
│   ├── utils/                    # Utilities
│   │   └── logging_config.py     # Structured logging
│   └── orchestrator.py           # Main entry point (CLI)
├── data/                         # Data directory (gitignored)
│   ├── input/                    # Control projects (Excel + .md)
│   │   ├── CTRL-IAM-007/
│   │   │   ├── control-information.md
│   │   │   └── evidence.xlsx
│   │   └── CTRL-SOX-AP-004/
│   │       ├── control-information.md
│   │       └── vendor_invoices.xlsx
│   ├── parquet/                  # Converted evidence (Parquet)
│   ├── logs/                     # Execution logs
│   └── audit.db                  # SQLite audit ledger
├── tests/                        # Test suite
│   ├── test_dsl_models.py        # DSL validation tests
│   ├── test_compiler.py          # SQL compilation tests
│   ├── test_integration.py       # End-to-end tests
│   └── test_orchestrator.py      # Orchestrator logic tests
├── docs/                         # Documentation
│   ├── COMPREHENSIVE_GUIDE.md    # This file
│   ├── dsl-quick-reference.md    # DSL syntax examples
│   └── orchestrator-guide.md     # Orchestrator usage
├── .env                          # Environment variables (API keys)
├── requirements.txt              # Python dependencies
├── README.md                     # Project overview
└── SETUP.md                      # Detailed setup instructions
```

### Control Project Structure

Each control lives in its own folder under `data/input/`:

```
data/input/CTRL-IAM-007/
├── control-information.md         # Required: Verification procedure
├── user_access_log.xlsx           # Evidence file 1
└── exceptions_report.xlsx         # Evidence file 2 (optional)
```

**`control-information.md` Format:**

```markdown
# Control Testing Steps for CTRL-IAM-007

## Control Objective
Ensure all users have appropriate access permissions and are reviewed quarterly.

## Verification Procedure
1. Extract user access log from IAM system
2. Verify each user has valid manager_id
3. Check last_review_date is within 90 days
4. Flag any admin users without MFA enabled
5. Calculate exception rate and assert <5% threshold
```

---

## Orchestrator Usage

### Command-Line Interface

```bash
# Production mode (real AI)
python -m src.orchestrator --input data/input --db data/audit.db

# Test mode (mock AI, no API key required)
python -m src.orchestrator --input data/input --mock-ai

# Custom paths
python -m src.orchestrator \
    --input /path/to/controls \
    --db /path/to/custom_audit.db
```

### Programmatic Usage

```python
from src.orchestrator import BatchOrchestrator

# Initialize orchestrator
orchestrator = BatchOrchestrator(
    use_mock_ai=False,              # Set True for testing
    db_path="data/audit.db",
    parquet_dir="data/parquet",
    enable_llm_validation=True      # Enable pre-flight LLM validation
)

# Process all projects
summary = orchestrator.process_all_projects("data/input")

# Clean up
orchestrator.close()

# Check results
print(f"Pass: {summary['pass_count']}")
print(f"Fail: {summary['fail_count']}")
print(f"Errors: {summary['error_count']}")
```

### Orchestrator Workflow

For each project, the orchestrator executes:

1. **Read `control-information.md`** → Extract control ID and procedure text
2. **Check DSL Cache** → Query audit database for existing DSL
3. **AI Generation (if needed)** → Call DeepSeek to generate DSL from headers
4. **Ingest Evidence** → Convert Excel → Parquet with SHA-256 hashing
5. **Optional LLM Validation** → Pre-flight DSL/SQL review (if enabled)
6. **SQL Validation** → DuckDB EXPLAIN dry-run (the "Strict Judge")
7. **Self-Healing (if failed)** → Feed error back to AI for DSL correction
8. **Execute Control** → Run DuckDB query against Parquet files
9. **Save Results** → Store in audit ledger with cryptographic hashes

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All controls passed |
| 1 | One or more controls failed or errored |
| 2 | Fatal orchestrator error (config issue, etc.) |

---

## DSL Reference

### Core Structure

```json
{
  "governance": {
    "control_id": "CTRL-IAM-007",
    "control_description": "User access review control",
    "owner": "IT Security",
    "version": "1.0.0",
    "effective_date": "2026-01-01"
  },
  "datasets": [...],      // Data sources
  "pipeline": [...],      // Transformation steps
  "assertions": [...],    // Validation rules
  "sampling": {...}       // Optional: statistical sampling
}
```

### Assertion Types

#### 1. Value Match (Field = Value)

```json
{
  "assertion_id": "status_must_be_active",
  "assertion_type": "value_match",
  "field": "status",
  "operator": "eq",
  "expected_value": "ACTIVE",
  "materiality_threshold_percent": 0.0
}
```

**Operators:** `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `not_contains`

#### 2. NULL Comparisons

```json
{
  "assertion_id": "approval_must_exist",
  "assertion_type": "value_match",
  "field": "approval_date",
  "operator": "neq",
  "expected_value": null,         // Compiles to IS NOT NULL
  "materiality_threshold_percent": 0.0
}
```

#### 3. Date Math (Field ≤ Field + N Days)

```json
{
  "assertion_id": "edd_timeliness",
  "assertion_type": "temporal_date_math",
  "base_date_field": "edd_completion_date",
  "operator": "lte",
  "target_date_field": "onboarding_date",
  "offset_days": 14,
  "materiality_threshold_percent": 0.0
}
```

**Generates:** `edd_completion_date <= onboarding_date + INTERVAL 14 DAY`

#### 4. Column-to-Column Comparison

```json
{
  "assertion_id": "gross_vs_net",
  "assertion_type": "column_to_column",
  "left_field": "gross_amount",
  "operator": "gte",
  "right_field": "net_amount",
  "materiality_threshold_percent": 0.0
}
```

#### 5. Aggregation Assertion

```json
{
  "assertion_id": "total_risk_score",
  "assertion_type": "aggregate",
  "aggregation_function": "AVG",
  "field": "risk_score",
  "group_by_fields": ["department"],
  "operator": "lte",
  "threshold_value": 75.0,
  "materiality_threshold_percent": 0.0
}
```

**Supported Functions:** `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`

#### 6. Referential Integrity (Foreign Key Check)

```json
{
  "assertion_id": "valid_manager",
  "assertion_type": "referential",
  "foreign_key_field": "manager_id",
  "reference_dataset": "managers",
  "reference_key_field": "employee_id",
  "materiality_threshold_percent": 0.0
}
```

### Pipeline Operations

#### 1. Filter

```json
{
  "operation": "filter",
  "filter_expression": "status == 'ACTIVE' AND department != 'TERMINATED'"
}
```

#### 2. Join

```json
{
  "operation": "join",
  "join_type": "LEFT",
  "right_dataset": "managers",
  "join_conditions": [
    {"left_field": "manager_id", "right_field": "employee_id"}
  ]
}
```

**Composite Joins:**

```json
{
  "operation": "join",
  "join_type": "INNER",
  "right_dataset": "trades",
  "join_conditions": [
    {"left_field": "account_id", "right_field": "account_id"},
    {"left_field": "trade_date", "right_field": "settlement_date"}
  ]
}
```

#### 3. Computed Field

```json
{
  "operation": "computed_field",
  "new_field_name": "days_since_review",
  "expression": "DATEDIFF('day', last_review_date, CURRENT_DATE)"
}
```

### Sampling Configuration

```json
{
  "sampling": {
    "enabled": true,
    "sample_type": "statistical",
    "confidence_level": 0.95,
    "precision": 0.05,
    "min_sample_size": 100,
    "max_sample_size": 500
  }
}
```

---

## AI Translation & Validation

### How AI Translation Works

1. **Input:** Plain English verification procedure + Excel column headers
2. **Process:** DeepSeek LLM generates JSON matching Pydantic DSL schema
3. **Safety:** AI never sees row data (PII-safe), only column names
4. **Output:** Validated DSL with discriminated unions (no hallucination)

### Schema Pruning

Before calling the AI, only relevant column names are exposed:

```python
# Safe: Only headers extracted
headers = {
    "user_access_log_sheet1": ["user_id", "manager_id", "status", "last_review_date", "mfa_enabled"]
}

# AI receives:
# - control-information.md (plain English)
# - headers (column names only, no data)

# AI never sees:
# - Actual user IDs, names, or sensitive data
# - Row-level values
```

### Optional LLM Validation (Double-Check Layer)

Enabled via `enable_llm_validation=True`:

```python
orchestrator = BatchOrchestrator(
    enable_llm_validation=True  # Enable pre-flight checks
)
```

**What it does:**
- Reviews generated DSL for logic errors
- Checks SQL for syntax issues, incorrect joins, missing filters
- Reports critical/warning/info issues
- **Does NOT block execution** (DuckDB is the final judge)

**Use cases:**
- High-risk controls (SOX 404)
- First-time control testing
- Complex multi-table joins

---

## Self-Healing System

### The Problem

AI-generated DSL might contain errors that only DuckDB can detect:
- Invalid column references
- Type mismatches
- Syntax errors in computed fields

### The Solution: Conditional Self-Healing

```
┌──────────────────┐
│   AI Generates   │
│       DSL        │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Compiler → SQL   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌──────────────┐
│ DuckDB EXPLAIN   │─PASS│  Execute     │
│  (Strict Judge)  │────▶│  Control     │
└────────┬─────────┘     └──────────────┘
         │
         │ FAIL
         ▼
┌──────────────────┐
│  Feed Error to   │
│  AI for Healing  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Re-compile &    │
│  Re-validate     │
└────────┬─────────┘
         │
         ├──PASS──▶ Execute
         │
         └──FAIL──▶ Error (human intervention)
```

### Example Self-Healing Flow

**Step 1: AI generates DSL**
```json
{
  "field": "last_review_dat",  // Typo!
  "operator": "lte",
  "expected_value": 90
}
```

**Step 2: DuckDB rejects SQL**
```
Error: Column 'last_review_dat' does not exist
Available: ['user_id', 'manager_id', 'last_review_date', 'status']
```

**Step 3: AI heals DSL**
```json
{
  "field": "last_review_date",  // Corrected
  "operator": "lte",
  "expected_value": 90
}
```

**Step 4: Re-validation succeeds → Execution proceeds**

---

## Audit Trail & Compliance

### Audit Database Schema

```sql
-- Control DSL versions
CREATE TABLE controls (
    control_id TEXT PRIMARY KEY,
    control_dsl TEXT,              -- JSON DSL
    version TEXT,
    approved_by TEXT,
    created_at TEXT
);

-- Evidence manifests (SHA-256 hashes)
CREATE TABLE evidence_manifests (
    manifest_id TEXT PRIMARY KEY,
    dataset_alias TEXT,
    source_path TEXT,
    parquet_path TEXT,
    sha256_hash TEXT,              -- Cryptographic verification
    row_count INTEGER,
    created_at TEXT
);

-- Execution history (immutable ledger)
CREATE TABLE executions (
    execution_id TEXT PRIMARY KEY,
    control_id TEXT,
    verdict TEXT,                  -- PASS/FAIL/ERROR
    total_population INTEGER,
    exception_count INTEGER,
    exception_rate_percent REAL,
    sql_query TEXT,                -- Exact SQL executed
    executed_at TEXT,
    FOREIGN KEY (control_id) REFERENCES controls(control_id)
);
```

### SHA-256 Hash Verification

Every evidence file is cryptographically hashed:

```python
# During ingestion
manifest = {
    "dataset_alias": "user_access_log_sheet1",
    "sha256_hash": "a3f2c1d8e6b4f9a7...",  # 64-char hex
    "row_count": 5000,
    "parquet_path": "data/parquet/user_access_log_sheet1.parquet"
}
```

**Audit defensibility:**
- If Parquet file is modified, hash changes
- Auditors can re-hash files to verify tampering
- All executions are linked to specific evidence hashes

### Querying Audit History

```python
from src.storage.audit_fabric import AuditFabric

audit = AuditFabric(db_path="data/audit.db")

# Get control DSL
dsl_dict = audit.get_control("CTRL-IAM-007")

# Get execution history
executions = audit.get_execution_history("CTRL-IAM-007", limit=10)

for exec in executions:
    print(f"{exec['executed_at']}: {exec['verdict']} "
          f"({exec['exception_count']}/{exec['total_population']})")
```

---

## Development Guide

### Adding a New Assertion Type

1. **Define Pydantic Model** in [src/models/dsl.py](src/models/dsl.py#L1):

```python
class CustomAssertion(BaseModel):
    assertion_id: str
    assertion_type: Literal["custom_check"]
    field: str
    custom_param: str
    materiality_threshold_percent: float = Field(ge=0.0, le=100.0)
```

2. **Add to Discriminated Union**:

```python
ControlAssertion = Annotated[
    Union[
        ValueMatchAssertion,
        CustomAssertion,  # Add here
        # ... other assertions
    ],
    Field(discriminator="assertion_type")
]
```

3. **Update Compiler** in [src/compiler/sql_compiler.py](src/compiler/sql_compiler.py#L1):

```python
def _compile_assertion(self, assertion: ControlAssertion) -> str:
    if assertion.assertion_type == "custom_check":
        return self._compile_custom_check(assertion)
    # ... handle other types

def _compile_custom_check(self, assertion: CustomAssertion) -> str:
    field = assertion.field
    param = self._escape_value(assertion.custom_param)
    return f"{field} <custom_logic> {param}"
```

4. **Write Tests**:

```python
def test_custom_assertion():
    dsl_data = {
        "governance": {...},
        "datasets": [...],
        "assertions": [
            {
                "assertion_type": "custom_check",
                "field": "test_field",
                "custom_param": "test_value",
                "materiality_threshold_percent": 5.0
            }
        ]
    }
    dsl = EnterpriseControlDSL(**dsl_data)
    compiler = ControlCompiler(dsl)
    sql = compiler.compile_to_sql(manifests)
    assert "custom_logic" in sql
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_dsl_models.py -v

# Run with coverage
pytest --cov=src --cov-report=html
```

### Logging Configuration

Logs are written to `data/logs/` with rotation:

```python
from src.utils.logging_config import get_logger

logger = get_logger(__name__)
logger.info("Processing control")
logger.warning("Potential issue detected")
logger.error("Critical error", exc_info=True)
```

---

## Troubleshooting

### Common Issues

#### 1. "DEEPSEEK_API_KEY not found"

**Solution:** Create `.env` file in project root:
```bash
echo "DEEPSEEK_API_KEY=your-key-here" > .env
```

Or use mock AI:
```bash
python -m src.orchestrator --mock-ai
```

#### 2. "No Excel files found - skipping project"

**Cause:** Project folder missing `.xlsx` or `.xls` files

**Solution:** Ensure evidence files are in the same folder as `control-information.md`

#### 3. "SQL validation failed"

**Possible causes:**
- Column name mismatch (AI typo)
- Invalid SQL expression in computed field
- Type mismatch in comparison

**What happens:**
- System triggers self-healing automatically
- AI receives exact DuckDB error message
- DSL is corrected and re-compiled

**Manual intervention needed if:**
- Self-healing fails (persistent error)
- Check logs in `data/logs/` for details

#### 4. "Validation Error: discriminator ... cannot be evaluated"

**Cause:** Invalid `assertion_type` or `operation` value in DSL

**Solution:** Ensure assertion/operation types match Pydantic `Literal` types exactly:
- Valid: `"value_match"`, `"temporal_date_math"`, `"aggregate"`
- Invalid: `"value-match"`, `"date_math"`, `"aggregation"`

#### 5. High Exception Rate (FAIL verdict)

**Interpretation:**
- PASS: Exceptions ≤ materiality threshold
- FAIL: Exceptions > materiality threshold

**Actions:**
1. Review exceptions in audit database
2. Check if control logic is correct
3. Verify evidence data quality
4. Consider adjusting materiality threshold

### Debug Mode

Enable verbose logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

orchestrator = BatchOrchestrator(...)
summary = orchestrator.process_all_projects()
```

### Performance Tuning

For large datasets (1M+ rows):

1. **Use DuckDB memory limit:**
```python
engine = ExecutionEngine()
engine.conn.execute("SET memory_limit='4GB'")
```

2. **Enable Parquet compression:**
```python
ingestion = EvidenceIngestion(storage_dir="data/parquet")
# Already uses 'snappy' compression by default
```

3. **Disable LLM validation for production:**
```python
orchestrator = BatchOrchestrator(
    enable_llm_validation=False  # Faster execution
)
```

---

## Additional Resources

- **DSL Quick Reference**: [docs/dsl-quick-reference.md](dsl-quick-reference.md)
- **Orchestrator Guide**: [docs/orchestrator-guide.md](orchestrator-guide.md)
- **Setup Instructions**: [SETUP.md](../SETUP.md)
- **Project README**: [README.md](../README.md)

---

## Support & Contributing

For issues or feature requests, please contact the development team or submit a GitHub issue.

**Project Status:** Production Ready ✅  
**Last Audit:** February 15, 2026  
**Compliance:** SOX 404, GDPR, Basel III Compatible
