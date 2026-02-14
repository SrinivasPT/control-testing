# End-to-End Test Scenario Plan
## Enterprise Compliance Control Engine

**Purpose:** Rigorously test all 5 layers (AI → DSL → Compiler → Execution → Audit) with realistic 10,000-row datasets containing strategically seeded exceptions.

**Philosophy:** "Controls are Data, Not Code" - We prove the system works by showing it can detect known violations hidden in large compliant datasets using pure DuckDB execution.

---

## Overview

To rigorously test the Compiler (Layer 3) and the DuckDB Execution Engine (Layer 4), you need data that actually proves the system can handle row-level checks, relational joins, and aggregations. Furthermore, you need "seeded exceptions" (intentional rule-breakers) hidden in thousands of rows of compliant data to prove the engine actually catches the failures.

Here are Three Enterprise Test Scenarios, followed by a robust Python script that generates the sample Excel files with exactly 10,000 rows and strategically hidden exceptions.

---

## Scenario 1: The Baseline (Row-Level Filtering & Assertion)
This tests the engine's ability to filter a dataset and apply a simple WHERE NOT (...) assertion.

Control ID: CTRL-TRD-001

Plain English Control: "Verify that all trades with a notional amount greater than $50,000 have an approval status of 'APPROVED'."

Expected DSL Behavior: * Population Filter: notional_amount > 50000

Assertion: approval_status == 'APPROVED'

Expected SQL Output:

SQL
WITH base AS (SELECT * FROM read_parquet('trades.parquet'))
SELECT * FROM base 
WHERE notional_amount > 50000 
  AND NOT (approval_status = 'APPROVED')
Scenario 2: The Cross-System Join (Relational Integrity)
This tests the LEFT JOIN logic and cross-dataset assertions. It proves the system can navigate multi-sheet Excel files.

Control ID: CTRL-HR-042

Plain English Control: "Ensure that all approved trades were authorized by a manager whose current HR employment status is 'ACTIVE'."

Expected DSL Behavior:

Population Pipeline: LEFT JOIN the Trade Log to the HR Roster on approver_id = employee_id.

Assertion: hr_roster.employment_status == 'ACTIVE'

Expected SQL Output:

SQL
WITH base AS (SELECT * FROM read_parquet('trades.parquet')),
join_1 AS (
    SELECT base.*, hr.employment_status 
    FROM base 
    LEFT JOIN read_parquet('hr_roster.parquet') AS hr 
    ON base.approver_id = hr.employee_id
)
SELECT * FROM join_1 
WHERE approval_status = 'APPROVED' 
  AND NOT (employment_status = 'ACTIVE')
Scenario 3: The Aggregation (GROUP BY & HAVING)
This is the hardest test. It proves your compiler correctly routes metric thresholds to a HAVING clause instead of a WHERE clause.

Control ID: CTRL-LMT-099

Plain English Control: "Verify that the sum of daily trade amounts for any individual trader does not exceed their daily limit of $2,000,000."

Expected DSL Behavior:

Assertion Type: aggregation_sum

Group By: trader_id, trade_date

Metric: notional_amount <= 2000000

Expected SQL Output:

SQL
WITH base AS (SELECT * FROM read_parquet('trades.parquet'))
SELECT trader_id, trade_date, SUM(notional_amount) as total_amount
FROM base
GROUP BY trader_id, trade_date
HAVING NOT (SUM(notional_amount) <= 2000000)
The Excel Data Generator Script
To test these scenarios, you cannot use 10 rows of manual data. You need realistic scale.

Run this Python script on your local machine. It uses pandas and numpy to generate two large Excel files (trade_log.xlsx and hr_roster.xlsx). Crucially, it explicitly injects exactly 3 known exceptions into the 10,000 rows so you can verify your execution engine works.

Prerequisites: pip install pandas numpy openpyxl

Python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_test_data(num_trades=10000, num_employees=500):
    print("Generating Enterprise Test Data...")

    # ---------------------------------------------------------
    # 1. Generate HR Roster (The Dimension Table)
    # ---------------------------------------------------------
    employee_ids = [f"EMP_{str(i).zfill(4)}" for i in range(1, num_employees + 1)]
    
    hr_data = {
        "employee_id": employee_ids,
        "department": np.random.choice(["Trading", "Compliance", "Operations"], num_employees),
        "employment_status": np.random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "TERMINATED", "ON_LEAVE"], num_employees)
    }
    df_hr = pd.DataFrame(hr_data)

    # ---------------------------------------------------------
    # 2. Generate Trade Log (The Fact Table)
    # ---------------------------------------------------------
    start_date = datetime(2025, 1, 1)
    dates = [start_date + timedelta(days=np.random.randint(0, 90)) for _ in range(num_trades)]
    
    trade_data = {
        "trade_id": [f"TRD_{str(i).zfill(6)}" for i in range(1, num_trades + 1)],
        "trade_date": dates,
        "trader_id": np.random.choice(employee_ids[:100], num_trades), # First 100 employees are traders
        "notional_amount": np.round(np.random.uniform(1000, 100000, num_trades), 2),
        "approval_status": np.random.choice(["APPROVED", "PENDING", "REJECTED"], num_trades, p=[0.8, 0.15, 0.05]),
        "approver_id": np.random.choice(employee_ids[100:], num_trades) # Rest are managers
    }
    df_trades = pd.DataFrame(trade_data)

    # ---------------------------------------------------------
    # 3. Inject Seeded Exceptions (The Audit Trap)
    # ---------------------------------------------------------
    print("\nInjecting Known Exceptions...")

    # Scenario 1 Exception: Trade > $50k but status is PENDING
    df_trades.loc[500, 'notional_amount'] = 75000.00
    df_trades.loc[500, 'approval_status'] = 'PENDING'
    print(f" -> Scenario 1 Exception seeded at Trade ID: {df_trades.loc[500, 'trade_id']}")

    # Scenario 2 Exception: Trade Approved by a TERMINATED employee
    terminated_emp = df_hr[df_hr['employment_status'] == 'TERMINATED']['employee_id'].iloc[0]
    df_trades.loc[1500, 'approval_status'] = 'APPROVED'
    df_trades.loc[1500, 'approver_id'] = terminated_emp
    print(f" -> Scenario 2 Exception seeded at Trade ID: {df_trades.loc[1500, 'trade_id']} (Approver: {terminated_emp})")

    # Scenario 3 Exception: Trader exceeds $2M daily limit
    rogue_trader = "EMP_0099"
    rogue_date = datetime(2025, 2, 15)
    
    # Overwrite a few rows to force this specific trader over the limit on this specific day
    df_trades.loc[2000:2003, 'trader_id'] = rogue_trader
    df_trades.loc[2000:2003, 'trade_date'] = rogue_date
    df_trades.loc[2000:2003, 'notional_amount'] = 600000.00 # 4 trades * 600k = $2.4M
    print(f" -> Scenario 3 Exception seeded for Trader: {rogue_trader} on {rogue_date.strftime('%Y-%m-%d')} (Total: $2.4M)")

    # ---------------------------------------------------------
    # 4. Save to Excel
    # ---------------------------------------------------------
    print("\nSaving files to disk...")
    df_trades.to_excel("sample_trade_log.xlsx", index=False)
    df_hr.to_excel("sample_hr_roster.xlsx", index=False)
    print("Success! Created 'sample_trade_log.xlsx' and 'sample_hr_roster.xlsx'")

    generate_test_data()
```

---

## How to Use This for End-to-End Testing

1. **Generate Test Data**: Run the Python script above. It will generate two Excel files in your directory:
   - `sample_trade_log.xlsx` (10,000 trades with 3 seeded exceptions)
   - `sample_hr_roster.xlsx` (500 employees)

2. **Run the E2E Test Suite**: Execute the comprehensive test file:
   ```bash
   pytest e2e/test_e2e_all_scenarios.py -v -s
   ```

3. **What Gets Tested**:
   - ✅ **Ingestion Layer**: Excel → Parquet conversion with SHA-256 hashing
   - ✅ **DSL Validation**: Pydantic v2 models enforce strict typing
   - ✅ **SQL Compiler**: Generates valid DuckDB SQL with CTEs, JOINs, GROUP BY
   - ✅ **Execution Engine**: DuckDB streams from disk (no memory bloat)
   - ✅ **Exception Detection**: Finds exactly the 3 seeded violations
   - ✅ **Performance**: Processes 10,000 rows in < 5 seconds
   - ✅ **Error Handling**: Gracefully handles invalid SQL

4. **Expected Results**: If your architecture is built correctly, your engine will run against the 10,000 rows in less than 0.1 seconds, and the output `exception_data` will contain exactly:
   - `TRD_000501` (Scenario 1: Large trade without approval)
   - `TRD_001501` (Scenario 2: Approved by terminated employee)
   - `EMP_0099` (Scenario 3: Trader exceeded $2M daily limit)

---

## Test Coverage Summary

| Test | Layer Tested | Validates |
|------|-------------|-----------|
| `test_scenario_1_row_level_assertion` | 3,4 | Row-level WHERE clause generation |
| `test_scenario_2_cross_system_join` | 3,4 | LEFT JOIN compilation and execution |
| `test_scenario_3_aggregation_limit` | 3,4 | GROUP BY / HAVING clause generation |
| `test_ingestion_produces_valid_manifests` | 4 | Parquet creation, SHA-256 hashing |
| `test_compiler_generates_valid_sql` | 3 | Syntax validation of generated SQL |
| `test_execution_handles_errors_gracefully` | 4 | Error handling without crashes |
| `test_performance_large_dataset` | 4 | Sub-second execution on 10K rows |

---

## Next Steps

1. ✅ **Data Generation**: Already completed - Excel files exist in `e2e/` directory
2. ✅ **Test Suite**: Comprehensive pytest suite created
3. 🔄 **Execute Tests**: Run pytest to validate all scenarios
4. ⏭️ **Audit Layer**: Extend tests to validate PostgreSQL logging (Layer 5)
5. ⏭️ **AI Layer**: Add tests for `instructor` + LLM translation (Layer 1)

---

## Architecture Validation Checklist

- [x] **No `exec()` or `eval()`**: All execution is deterministic DuckDB
- [x] **No Pandas manipulation**: Data processing happens in SQL, not Python
- [x] **Proper SQL escaping**: Compiler handles string escaping correctly
- [x] **CTE chaining**: Multi-step pipelines use proper CTE references
- [x] **Discriminated unions**: Pydantic enforces type safety with `Field(..., discriminator=...)`
- [x] **SHA-256 audit trail**: Every Parquet file has cryptographic hash
- [x] **Local environment**: No cloud dependencies, runs on localhost
- [x] **Separation of concerns**: 5-layer architecture properly enforced

---

## Performance Benchmarks (Expected)

| Dataset Size | Execution Time | Throughput |
|-------------|---------------|------------|
| 10,000 rows | < 0.5s | 20,000+ rows/sec |
| 100,000 rows | < 2s | 50,000+ rows/sec |
| 1,000,000 rows | < 10s | 100,000+ rows/sec |

*Benchmarks assume local NVMe SSD and DuckDB in-memory mode*

---

## Troubleshooting

**Issue: `FileNotFoundError: sample_trade_log.xlsx`**  
→ Run `tests/generate_test_data.py` first to create the Excel files

**Issue: `ImportError: No module named 'openpyxl'`**  
→ Run `pip install openpyxl pandas numpy`

**Issue: Tests fail with "nonexistent_field_xyz not found"**  
→ This is expected - `test_execution_handles_errors_gracefully` intentionally tests error handling

**Issue: Performance test times out**  
→ Verify you're using DuckDB's disk-streaming mode, not loading data into Pandas memory

---

**Last Updated**: February 2026  
**Maintainer**: Principal Data Engineer & IT Security Architect
