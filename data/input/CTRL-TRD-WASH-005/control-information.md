# Control ID: CTRL-TRD-WASH-005

## Risk Statement
Executing a transaction where the buyer and seller are the same entity constitutes Wash Trading, a form of market manipulation prohibited by the SEC and CFTC.

## Source Information
- daily_execution_blotter.xlsx (Log of all market executions)

## Approach Followed
1. Filter the blotter to executed trades (status = 'EXECUTED').
2. Compare the `buyer_account_id` to the `seller_account_id` on every trade.
3. Verify that the `buyer_account_id` does NOT equal the `seller_account_id`.