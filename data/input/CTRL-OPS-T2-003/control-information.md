# Control ID: CTRL-OPS-T2-003

## Risk Statement
Failure to settle equity trades within the standard T+2 (Trade Date + 2 days) window exposes the firm to counterparty credit risk and regulatory fines from the SEC/FINRA.

## Source Information
- equity_settlements.xlsx (Log of all executed and settled trades)

## Approach Followed
1. Filter the population to trades where the `trade_status` is 'SETTLED'.
2. Verify that the `settlement_date` occurred on or before the `trade_date` plus 2 days.