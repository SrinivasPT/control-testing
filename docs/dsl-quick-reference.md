# Quick Reference: New DSL Features

## Table of Contents
1. [NULL Comparisons](#null-comparisons)
2. [Date Math Assertions](#date-math-assertions)
3. [Column-to-Column Comparisons](#column-to-column-comparisons)
4. [Composite Joins](#composite-joins)
5. [Generalized Aggregations](#generalized-aggregations)
6. [Case-Insensitive Strings](#case-insensitive-strings)

---

## NULL Comparisons

### Problem
SQL doesn't allow `field = NULL` or `field != NULL` - these always return UNKNOWN.

### Solution
The compiler automatically converts NULL comparisons to `IS NULL` / `IS NOT NULL`.

### Example 1: Find Missing Values
```json
{
  "assertion_id": "missing_approval_date",
  "assertion_type": "value_match",
  "description": "Find records with missing approval dates",
  "field": "approval_date",
  "operator": "eq",
  "expected_value": null,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `WHERE approval_date IS NULL`

### Example 2: Ensure Values Exist
```json
{
  "assertion_id": "has_edd_date",
  "assertion_type": "value_match",
  "description": "Ensure EDD completion date exists",
  "field": "edd_completion_date",
  "operator": "neq",
  "expected_value": null,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `WHERE edd_completion_date IS NOT NULL`

---

## Date Math Assertions

### Problem
Comparing dates with offsets like "within 14 days" requires date arithmetic, not string literals.

### Solution
Use `TemporalDateMathAssertion` to generate proper INTERVAL syntax.

### Example 1: EDD Timeliness Check
```json
{
  "assertion_id": "edd_timeliness",
  "assertion_type": "temporal_date_math",
  "description": "EDD must complete within 14 days of onboarding",
  "base_date_field": "edd_completion_date",
  "operator": "lte",
  "target_date_field": "onboarding_date",
  "offset_days": 14,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `edd_completion_date <= onboarding_date + INTERVAL 14 DAY`

### Example 2: Cooldown Period
```json
{
  "assertion_id": "cooldown_check",
  "assertion_type": "temporal_date_math",
  "description": "Trade must occur at least 30 days after wall-cross",
  "base_date_field": "trade_date",
  "operator": "gte",
  "target_date_field": "wall_cross_date",
  "offset_days": 30,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `trade_date >= wall_cross_date + INTERVAL 30 DAY`

### Supported Operators
- `gt` - Greater than (after)
- `gte` - Greater than or equal (on or after)
- `lt` - Less than (before)
- `lte` - Less than or equal (on or before)
- `eq` - Exactly on date

---

## Column-to-Column Comparisons

### Problem
Comparing two dynamic columns (not static values) requires unquoted references.

### Solution
Use `ColumnComparisonAssertion` to compare columns directly.

### Example 1: Trade After Clearance
```json
{
  "assertion_id": "trade_after_clearance",
  "assertion_type": "column_comparison",
  "description": "Trade date must be after clearance date",
  "left_field": "trade_date",
  "operator": "gt",
  "right_field": "clearance_date",
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `trade_date > clearance_date`

### Example 2: Settlement Before Maturity
```json
{
  "assertion_id": "settlement_before_maturity",
  "assertion_type": "column_comparison",
  "description": "Settlement date must be before maturity date",
  "left_field": "settlement_date",
  "operator": "lt",
  "right_field": "maturity_date",
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `settlement_date < maturity_date`

### Example 3: Balance Matches Calculated
```json
{
  "assertion_id": "balance_reconciliation",
  "assertion_type": "column_comparison",
  "description": "Reported balance must equal calculated balance",
  "left_field": "reported_balance",
  "operator": "eq",
  "right_field": "calculated_balance",
  "materiality_threshold_percent": 0.5
}
```
**Generated SQL**: `reported_balance = calculated_balance`

---

## Composite Joins

### Problem
Joining on multiple columns (e.g., employee_id AND ticker_symbol) requires composite keys.

### Solution
Use lists in `left_keys` and `right_keys` instead of single strings.

### Example 1: MNPI Insider Trading Check
```json
{
  "step_id": "join_wall_cross_register",
  "action": {
    "operation": "join_left",
    "left_dataset": "personal_trade_blotter_sheet1",
    "right_dataset": "wall_cross_register_sheet1",
    "left_keys": ["employee_id", "ticker_symbol"],
    "right_keys": ["employee_id", "ticker_symbol"]
  }
}
```
**Generated SQL**:
```sql
LEFT JOIN read_parquet('wall_cross_register.parquet') AS right_tbl
ON base.employee_id = right_tbl.employee_id 
   AND base.ticker_symbol = right_tbl.ticker_symbol
```

### Example 2: Multi-Currency Position Check
```json
{
  "step_id": "join_fx_rates",
  "action": {
    "operation": "join_left",
    "left_dataset": "positions",
    "right_dataset": "fx_rates",
    "left_keys": ["currency_code", "valuation_date"],
    "right_keys": ["currency_code", "rate_date"]
  }
}
```

### Example 3: Single Key Join (Backward Compatible)
```json
{
  "step_id": "join_approvers",
  "action": {
    "operation": "join_left",
    "left_dataset": "trades",
    "right_dataset": "hr_roster",
    "left_keys": ["approver_id"],
    "right_keys": ["employee_id"]
  }
}
```
**Note**: Even single-key joins must use list format now.

---

## Generalized Aggregations

### Problem
Only `SUM` was supported, limiting ability to count, average, find min/max.

### Solution
Use `AggregationAssertion` with `aggregation_function` field.

### Example 1: Transaction Count Limit
```json
{
  "assertion_id": "max_trades_per_day",
  "assertion_type": "aggregation",
  "description": "Trader cannot execute more than 50 trades per day",
  "group_by_fields": ["trader_id", "trade_date"],
  "metric_field": "trade_id",
  "aggregation_function": "COUNT",
  "operator": "lte",
  "threshold": 50,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: 
```sql
SELECT trader_id, trade_date, COUNT(*) as exception_count
FROM base
GROUP BY trader_id, trade_date
HAVING NOT (COUNT(trade_id) <= 50)
```

### Example 2: Average Position Size
```json
{
  "assertion_id": "avg_position_size",
  "assertion_type": "aggregation",
  "description": "Average position per trader must not exceed $1M",
  "group_by_fields": ["trader_id"],
  "metric_field": "notional_usd",
  "aggregation_function": "AVG",
  "operator": "lte",
  "threshold": 1000000,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `HAVING NOT (AVG(notional_usd) <= 1000000)`

### Example 3: Total Exposure (Sum)
```json
{
  "assertion_id": "total_exposure",
  "assertion_type": "aggregation",
  "description": "Total firm exposure must not exceed $100M",
  "group_by_fields": ["firm_id"],
  "metric_field": "exposure_usd",
  "aggregation_function": "SUM",
  "operator": "lte",
  "threshold": 100000000,
  "materiality_threshold_percent": 0.0
}
```

### Example 4: Minimum Balance
```json
{
  "assertion_id": "min_balance",
  "assertion_type": "aggregation",
  "description": "Minimum account balance must be at least $1000",
  "group_by_fields": ["account_id"],
  "metric_field": "balance",
  "aggregation_function": "MIN",
  "operator": "gte",
  "threshold": 1000,
  "materiality_threshold_percent": 0.0
}
```

### Supported Functions
- `SUM` - Total amount
- `COUNT` - Number of records
- `AVG` - Average value
- `MIN` - Minimum value
- `MAX` - Maximum value

---

## Case-Insensitive Strings

### Problem
Manual data entry causes variations: "APPROVED" vs "Approved" vs " approved "

### Solution
`ValueMatchAssertion` automatically handles case/whitespace by default.

### Example 1: Status Check (Case-Insensitive)
```json
{
  "assertion_id": "approval_status",
  "assertion_type": "value_match",
  "description": "Status must be APPROVED",
  "field": "approval_status",
  "operator": "eq",
  "expected_value": "APPROVED",
  "ignore_case_and_space": true,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `TRIM(UPPER(CAST(approval_status AS VARCHAR))) = TRIM(UPPER('APPROVED'))`

**Matches**: "APPROVED", "Approved", "approved", " APPROVED ", "ApPrOvEd"

### Example 2: Exact Match (Case-Sensitive)
```json
{
  "assertion_id": "product_code",
  "assertion_type": "value_match",
  "description": "Product code must be exactly 'FX-USD-SPOT'",
  "field": "product_code",
  "operator": "eq",
  "expected_value": "FX-USD-SPOT",
  "ignore_case_and_space": false,
  "materiality_threshold_percent": 0.0
}
```
**Generated SQL**: `product_code = 'FX-USD-SPOT'`

**Matches**: Only "FX-USD-SPOT" (exact match)

### Default Behavior
- `ignore_case_and_space` defaults to `true`
- Applies to: `eq`, `neq`, `gt`, `lt`, `gte`, `lte`
- Does NOT apply to: `in`, `not_in` (list comparisons)

---

## Combining Features: Real-World Example

### Control: CTRL-MNPI-707 (Fixed)
Prevent insider trading by ensuring employees don't trade on tickers they're restricted on.

```json
{
  "population": {
    "base_dataset": "personal_trade_blotter_sheet1",
    "steps": [
      {
        "step_id": "join_wall_cross_register",
        "action": {
          "operation": "join_left",
          "left_dataset": "personal_trade_blotter_sheet1",
          "right_dataset": "wall_cross_register_sheet1",
          "left_keys": ["employee_id", "ticker_symbol"],
          "right_keys": ["employee_id", "ticker_symbol"]
        }
      },
      {
        "step_id": "filter_restricted_only",
        "action": {
          "operation": "filter_is_null",
          "field": "wall_cross_register_sheet1.restriction_status",
          "is_null": false
        }
      }
    ]
  },
  "assertions": [
    {
      "assertion_id": "must_be_cleared",
      "assertion_type": "value_match",
      "description": "Restriction status must be CLEARED",
      "field": "wall_cross_register_sheet1.restriction_status",
      "operator": "eq",
      "expected_value": "CLEARED",
      "ignore_case_and_space": true,
      "materiality_threshold_percent": 0.0
    },
    {
      "assertion_id": "trade_after_clearance",
      "assertion_type": "column_comparison",
      "description": "Trade date must be after clearance date",
      "left_field": "personal_trade_blotter_sheet1.trade_date",
      "operator": "gt",
      "right_field": "wall_cross_register_sheet1.clearance_date",
      "materiality_threshold_percent": 0.0
    }
  ]
}
```

**This example demonstrates**:
1. ✅ Composite join (employee_id + ticker_symbol)
2. ✅ NULL filtering (only check records with restrictions)
3. ✅ Case-insensitive strings (CLEARED vs Cleared)
4. ✅ Column-to-column comparison (trade_date > clearance_date)
5. ✅ Qualified column names (dataset_alias.column_name)

---

## Best Practices

### 1. Always Use Qualified Column Names After Joins
```json
// Good:
"field": "hr_roster_sheet1.status"

// Bad (will crash if both tables have 'status'):
"field": "status"
```

### 2. Use Composite Joins When Needed
```json
// Good - Precise join:
"left_keys": ["employee_id", "ticker_symbol"]

// Bad - Cartesian explosion:
"left_keys": ["employee_id"]
// Then trying to filter: "ticker_symbol = ticker_symbol"
```

### 3. Choose the Right Assertion Type
- Static value check → `value_match`
- Date arithmetic → `temporal_date_math`
- Column comparison → `column_comparison`
- Aggregation → `aggregation`

### 4. Handle NULLs in Left Joins
When joining to optional tables (e.g., sanctions lists), add a filter:
```json
{
  "action": {
    "operation": "filter_is_null",
    "field": "sanctions_list.id",
    "is_null": false
  }
}
```
This ensures you only check records that match.

---

## Migration Checklist

- [ ] Update all `JoinLeft` to use `left_keys`/`right_keys` arrays
- [ ] Replace date math strings with `TemporalDateMathAssertion`
- [ ] Replace column comparisons with `ColumnComparisonAssertion`
- [ ] Update aggregations to use `AggregationAssertion` with proper function
- [ ] Add NULL filtering after LEFT JOINs where appropriate
- [ ] Use qualified column names in assertions after joins
- [ ] Test with case variations in test data

---

## Need Help?

See full documentation:
- [Implementation Summary](gemini-review-fixes.md)
- [DSL Schema](../src/models/dsl.py)
- [Test Examples](../tests/test_new_features.py)
