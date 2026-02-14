# Enterprise Compliance Control Operating System

## Overview

A bank-grade platform that automates the verification of 1,000+ compliance controls against large-scale evidence datasets. The system uses AI strictly as a semantic translator to convert plain-English controls into a rigidly typed Domain Specific Language (DSL), ensuring 100% audit defensibility through deterministic execution.

## Key Features

- **AI-Powered Translation**: Converts plain English controls to validated DSL
- **Type-Safe DSL**: Pydantic v2 with discriminated unions prevents hallucination
- **Deterministic Execution**: DuckDB disk-streaming for memory-safe processing
- **Cryptographic Audit Trail**: SHA-256 hashing of evidence with SQLite ledger
- **SOX Compliant**: Supports statistical sampling and immutable execution history

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: AI Semantic Translator (Schema Pruning)           │
│  Layer 2: Canonical Control DSL (Pydantic v2)              │
│  Layer 3: SQL Compiler (CTE Chaining + Value Escaping)     │
│  Layer 4: Execution Engine (DuckDB Disk Streaming)         │
│  Layer 5: Audit Fabric (SQLite with Hash Verification)     │
└─────────────────────────────────────────────────────────────┘
```

## Installation

```bash
# Clone repository
git clone <repo-url>
cd control-tester

# Install dependencies
pip install -r requirements.txt

# Set up environment
export OPENAI_API_KEY="your-api-key"  # Optional, for AI translation
```

## Quick Start

### 1. Production Batch Processing (Recommended)

```bash
# Run orchestrator on all projects in data/input/
python -m src.orchestrator --input data/input

# Use mock AI for testing (no API key required)
python -m src.orchestrator --input data/input --mock-ai
```

**Expected folder structure:**

```
data/input/
├── P1000/
│   ├── control-information.md   # Plain English verification procedure
│   ├── trade_log.xlsx           # Evidence file 1
│   └── approvals.xlsx           # Evidence file 2
└── P1001/
    ├── control-information.md
    └── user_access.xlsx
```

The orchestrator will:
1. ✅ Check if DSL exists in database (instant if cached)
2. 🤖 Generate DSL via AI if missing (only sees headers, never PII)
3. 📊 Convert Excel → Parquet with SHA-256 hashing
4. ⚙️ Execute deterministic DuckDB tests
5. 💾 Store results in immutable audit ledger

**See [docs/orchestrator-guide.md](docs/orchestrator-guide.md) for detailed usage.**

### 2. Programmatic Usage (Without AI)

```python
from src.models.dsl import EnterpriseControlDSL
from src.execution.ingestion import EvidenceIngestion
from src.execution.engine import ExecutionEngine
from src.storage.audit_fabric import AuditFabric

# Initialize components
ingestion = EvidenceIngestion(storage_dir="data/parquet")
engine = ExecutionEngine()
audit = AuditFabric(db_path="data/audit.db")

# Load a control DSL (from JSON file)
with open("controls/sample_control.json") as f:
    dsl_dict = json.load(f)
    dsl = EnterpriseControlDSL(**dsl_dict)

# Ingest evidence
manifests = ingestion.ingest_excel_to_parquet(
    excel_path="evidence/trade_log.xlsx",
    dataset_prefix="trade_log",
    source_system="SAP_FI"
)

# Execute control
report = engine.execute_control(dsl, {m['dataset_alias']: m for m in manifests})

# Save to audit ledger
audit.save_execution(report)

# Print results
print(f"Verdict: {report['verdict']}")
print(f"Exceptions: {report['exception_count']} / {report['total_population']}")
```

### 3. With AI Translation

```python
from src.ai.translator import AITranslator
from src.execution.ingestion import EvidenceIngestion

# Initialize translator
translator = AITranslator(api_key="your-openai-key")

# Get evidence headers
ingestion = EvidenceIngestion()
headers = ingestion.get_column_headers("evidence/trade_log.xlsx")

# Translate control
control_text = """
Ensure all trades with notional amount exceeding $10,000 executed in Q3 2025
have been approved by an active manager (employment status = 'ACTIVE').
"""

dsl = translator.translate_control(control_text, headers)

# Save approved DSL
audit = AuditFabric()
audit.save_control(dsl.model_dump(), approved_by="john.doe@bank.com")
```

## Example DSL

```json
{
  "governance": {
    "control_id": "SOX-TRADE-001",
    "version": "1.0.0",
    "owner_role": "Trading Compliance",
    "testing_frequency": "Daily",
    "regulatory_citations": ["SOX 404", "MiFID II"],
    "risk_objective": "Ensure high-value trades are approved"
  },
  "ontology_bindings": [
    {
      "business_term": "Trade Amount",
      "dataset_alias": "trade_log",
      "technical_field": "notional_usd",
      "data_type": "numeric"
    }
  ],
  "population": {
    "base_dataset": "trade_log",
    "steps": [
      {
        "step_id": "filter_large",
        "action": {
          "operation": "filter_comparison",
          "field": "notional_usd",
          "operator": "gt",
          "value": 10000
        }
      }
    ]
  },
  "assertions": [
    {
      "assertion_id": "assert_001",
      "assertion_type": "value_match",
      "description": "Trade must be approved",
      "field": "approval_status",
      "operator": "eq",
      "expected_value": "APPROVED",
      "materiality_threshold_percent": 0.0
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "JIRA:COMPLIANCE"
  }
}
```

## Project Structure

```
control-tester/
├── src/
│   ├── orchestrator.py         # 🆕 Batch processor (production entry point)
│   ├── models/
│   │   └── dsl.py              # Pydantic DSL schemas
│   ├── ai/
│   │   └── translator.py       # AI translation with schema pruning
│   ├── compiler/
│   │   └── sql_compiler.py     # DSL to SQL compilation
│   ├── execution/
│   │   ├── ingestion.py        # Excel to Parquet conversion
│   │   └── engine.py           # DuckDB execution engine
│   └── storage/
│       └── audit_fabric.py     # SQLite audit ledger
├── tests/                      # Unit and integration tests
│   ├── test_orchestrator.py   # 🆕 End-to-end orchestrator tests
│   └── ...
├── data/
│   ├── input/                  # 🆕 Project folders (each = 1 control test)
│   │   ├── P1000/
│   │   │   ├── control-information.md
│   │   │   └── *.xlsx
│   │   └── P1001/
│   ├── parquet/                # Evidence storage
│   └── audit.db                # SQLite database
├── docs/
│ Test orchestrator (no API key required)
python tests/test_orchestrator.py

# Run all pytestrchestrator-guide.md  # 🆕 Detailed orchestratory usage
│   ├── end-to-end.md
│   └── refined_design.md
├── requirements.txt
└── README.md
```

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test
pytest tests/test_compiler.py -v
```

## Key Design Decisions

### 1. SQLite vs PostgreSQL
Using SQLite for simplicity and portability. For production:
- Supports concurrent reads
- File-based (no server required)
- Suitable for 1-10K controls
- Upgrade to PostgreSQL for distributed deployments

### 2. Critical Bug Fixes Implemented
- ✅ **CTE Chaining**: Each pipeline step references previous step (not always 'base')
- ✅ **SQL Injection**: Single quotes properly escaped (O'Connor → O''Connor)
- ✅ **Sampling Framework**: SOX-compliant statistical sampling support
- ✅ **Source Metadata**: Evidence manifests include extraction timestamps and schema versions

### 3. Deterministic Execution
- No AI in execution path (only translation)
- SQL is reproducible and auditable
- DuckDB streams from disk (no OOM errors)

## Performance Targets

| Metric | Target |
|--------|--------|
| Excel Ingestion (10K rows) | <10 seconds |
| SQL Compilation | <1 second |
| Execution (50K rows) | <5 seconds |
| End-to-End | <60 seconds |

## Security Considerations

- LLM only sees column headers (never PII data)
- Evidence files hashed with SHA-256
- Immutable audit trail (append-only)
- Role-based access control (future)

## Roadmap

- **Phase 1 (Weeks 1-4)**: Core engine (DSL, Compiler, Execution) ✅
- **Phase 2 (Weeks 5-6)**: AI integration
- **Phase 3 (Weeks 7-8)**: Web API (FastAPI)
- **Phase 4 (Weeks 9-10)**: Scheduler & JIRA routing
- **Phase 5 (Weeks 11-12)**: Security & audit reports

## Contributing

See [design.md](design.md) and [refined_design.md](refined_design.md) for comprehensive architectural documentation.

## License

Proprietary - Enterprise Compliance System

## Support

For questions or issues, contact the compliance engineering team.
