# Orchestrator Usage Guide

## Overview

The **BatchOrchestrator** is the production-ready entry point that ties together all 5 architectural layers:

```
┌─────────────────────────────────────────────────────┐
│                  ORCHESTRATOR                        │
│  (Loops through projects, coordinates all layers)   │
└─────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐     ┌──────────┐    ┌──────────┐
   │ Layer 1 │     │ Layer 3  │    │ Layer 5  │
   │   AI    │     │Ingestion │    │  Audit   │
   │Translator     │(Excel→   │    │  Fabric  │
   │(DeepSeek)│    │ Parquet) │    │(SQLite)  │
   └─────────┘     └──────────┘    └──────────┘
        │                │                │
        └────────────────┼────────────────┘
                         ▼
                   ┌──────────┐
                   │ Layer 2  │
                   │   DSL    │
                   │(Pydantic)│
                   └──────────┘
                         │
                         ▼
                   ┌──────────┐
                   │ Layer 4  │
                   │Execution │
                   │ (DuckDB) │
                   └──────────┘
```

## What It Does

For each project folder in `data/input/`:

1. **Read** `control-information.md` (plain English verification procedure)
2. **Check** if DSL exists in audit database (cache hit = instant)
3. **Generate** DSL via AI if not cached (only sees headers, never PII)
4. **Ingest** Excel evidence to Parquet with SHA-256 hashing
5. **Execute** deterministic SQL test via DuckDB (zero AI involvement)
6. **Store** results in immutable audit ledger

## Project Folder Structure

```
data/input/
├── P1000/
│   ├── control-information.md   ← Required: verification procedure
│   ├── trade_log.xlsx           ← Evidence file 1
│   └── approvals.xlsx           ← Evidence file 2
├── P1001/
│   ├── control-information.md
│   └── user_access.xlsx
└── P1002/
    ├── control-information.md
    ├── reconciliation.xlsx
    └── exceptions.xlsx
```

### Requirements per Project

| File/Folder | Required | Description |
|------------|----------|-------------|
| `control-information.md` | ✅ Yes | Contains control ID and verification steps |
| `*.xlsx` or `*.xls` | ✅ Yes | At least one Excel evidence file |

## Running the Orchestrator

### Method 1: Command Line (Production)

```bash
# Run with real AI (requires DEEPSEEK_API_KEY in .env)
python -m src.orchestrator --input data/input

# Run with mock AI (for testing without API key)
python -m src.orchestrator --input data/input --mock-ai

# Custom database path
python -m src.orchestrator --input data/input --db data/my_audit.db
```

### Method 2: Python Script

```python
from src.orchestrator import BatchOrchestrator

# Initialize (set use_mock_ai=True to skip real LLM calls)
orchestrator = BatchOrchestrator(
    use_mock_ai=False,  # Set to True for testing
    db_path="data/audit.db",
    parquet_dir="data/parquet"
)

# Process all projects
summary = orchestrator.process_all_projects("data/input")

# Print results
print(f"Total: {summary['total_projects']}")
print(f"Pass: {summary['pass_count']}")
print(f"Fail: {summary['fail_count']}")

# Cleanup
orchestrator.close()
```

## Output Example

```
============================================================
🚀 ENTERPRISE CONTROL ORCHESTRATOR
============================================================
Found 2 project(s) to process

────────────────────────────────────────────────────────────
📁 Processing Project: P1000
────────────────────────────────────────────────────────────

[1/5] 📄 Reading control-information.md...
   Control ID: CTRL-908101

[2/5] 📊 Scanning for Excel evidence files...
   Found 2 file(s): trade_log.xlsx, approvals.xlsx

[3/5] 🧠 Checking for existing DSL in audit database...
   ✓ DSL found (version 1.0.0) - reusing cached version

[4/5] 🔄 Ingesting Excel files to Parquet with cryptographic hashing...
   Processing: trade_log.xlsx...
      ✓ trade_log_sheet1: 1523 rows, hash: a3f5d8e2b1c4...
   Processing: approvals.xlsx...
      ✓ approvals_sheet1: 1520 rows, hash: 7b2e9c4a1f3d...

[5/5] ⚙️  Executing control via DuckDB SQL engine...
   ✓ Execution complete - results saved to audit database

✅ VERDICT: PASS
   Exceptions: 3/1523 (0.20%)

────────────────────────────────────────────────────────────
📁 Processing Project: P1001
────────────────────────────────────────────────────────────

[1/5] 📄 Reading control-information.md...
   Control ID: CTRL-908102

[2/5] 📊 Scanning for Excel evidence files...
   ⚠️  No Excel files found - skipping project

⏭️ VERDICT: SKIPPED

============================================================
📊 EXECUTION SUMMARY
============================================================
Total Projects:      2
  ✅ PASS:           1
  ❌ FAIL:           0
  ⚠️  ERROR:          0
  ⏭️  SKIPPED:        1

DSL Generation:
  🔄 Cached (DB):    1
  🤖 AI Generated:   0
============================================================
```

## Key Design Decisions

### 1. Safety: AI Never Sees Row Data

```python
# ✅ SAFE: Only column headers extracted
headers = self.ingestion.get_column_headers(excel_path)
# headers = {"Sheet1": ["employee_id", "salary", "ssn"]}

dsl = self.ai.translate_control(control_text, headers)
# AI sees column names, but never sees actual SSNs or salaries
```

### 2. Speed: DSL Caching

| Run | DSL Source | Time | Cost |
|-----|-----------|------|------|
| First run (Monday) | AI Generation | ~15s | $0.02 |
| Second run (Tuesday) | Database cache | ~0.5s | $0.00 |
| Third run (Wednesday) | Database cache | ~0.5s | $0.00 |

**Result:** 30x speedup + 100% cost savings on repeat runs

### 3. Repeatability: Cryptographic Hashing

Every Parquet file gets a SHA-256 fingerprint:

```python
{
    "dataset_alias": "trade_log_sheet1",
    "sha256_hash": "a3f5d8e2b1c4...",  # 64-char hex
    "row_count": 1523,
    "ingested_at": "2026-02-14T10:30:45"
}
```

**Audit Benefit:** If the evidence file changes between runs, the hash breaks, and auditors will see "EVIDENCE TAMPERING" alert.

## Draft vs. Approved DSLs

Currently, all AI-generated DSLs are saved with:

```python
approved_by="AUTO_GENERATED_SYSTEM"
```

**Enterprise Refinement (Future):**

In production banks, you would add a human QA step:

```python
# Step 1: AI generates DSL and marks as DRAFT
self.audit.save_control(dsl, approved_by=None, status="DRAFT")

# Step 2: Human reviews in web UI, approves/rejects
self.audit.approve_control(control_id, approved_by="john.doe@bank.com")

# Step 3: Only APPROVED DSLs are used for audit ledger
```

For POC/testing, auto-approval is perfectly acceptable.

## Integration with CI/CD

```yaml
# .github/workflows/daily-controls.yml
name: Daily Control Testing

on:
  schedule:
    - cron: '0 2 * * *'  # 2 AM daily

jobs:
  test-controls:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Run Control Tests
        env:
          DEEPSEEK_API_KEY: ${{ secrets.DEEPSEEK_API_KEY }}
        run: python -m src.orchestrator --input data/input
      
      - name: Upload Results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: audit-report
          path: data/audit.db
```

## Troubleshooting

### Error: "DEEPSEEK_API_KEY not found"

**Solution:** Create `.env` file in project root:

```bash
# .env
DEEPSEEK_API_KEY=your_api_key_here
```

Or use mock mode:

```bash
python -m src.orchestrator --mock-ai
```

### Error: "No Excel files found"

**Cause:** Project folder is empty or contains no `.xlsx`/`.xls` files

**Solution:** Add at least one Excel evidence file to the project folder

### Error: "control-information.md not found"

**Cause:** Required markdown file is missing

**Solution:** Create `control-information.md` with at least a control ID reference

### Performance: Slow on first run

**Cause:** AI generation takes 10-15 seconds per control

**Solution:** This is expected. Subsequent runs will be instant (DSL cached in database)

## Next Steps

1. **Add More Projects**: Copy project folders to `data/input/`
2. **Review Database**: Open `data/audit.db` with SQLite browser to inspect results
3. **Build Dashboard**: Query `executions` table for trend analysis
4. **Add Human Approval**: Modify `save_control()` to require QA signoff

## Architecture Benefits Recap

| Traditional Approach | This System |
|---------------------|-------------|
| Python code for each control | JSON DSL for all controls |
| Manual Excel parsing | Automatic Parquet conversion |
| Random spot checks | 100% population testing |
| No audit trail | Immutable SHA-256 ledger |
| 30-day dev time per control | 30-minute AI generation |
| Requires Python developer | Business analyst can write markdown |
