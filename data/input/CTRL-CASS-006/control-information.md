# Control ID: CTRL-CASS-006

## Risk Statement
Failure to adequately segregate and fund Client Money accounts violates the FCA CASS rules, risking severe penalties and loss of banking license.

## Source Information
- client_money_ledger.xlsx (Daily snapshot of client deposits and firm funding)

## Approach Followed
1. Filter the ledger to only evaluate rows where the `account_type` is 'CLIENT_FUNDS'.
2. Verify that the aggregated SUM of the `current_balance` grouped by `calculation_date` is strictly greater than or equal to $50,000,000 (The firm's mandatory minimum segregated buffer).