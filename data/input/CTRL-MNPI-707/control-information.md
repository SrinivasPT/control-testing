# Control ID: CTRL-MNPI-707

## Risk Statement
Employees exposed to Material Non-Public Information (MNPI) executing personal trades on restricted tickers violates SEC Rule 10b-5 (Insider Trading).

## Source Information
Evidence provided by the Control Room and Employee Compliance:
- wall_cross_register.xlsx (Log of employees exposed to MNPI per ticker)
- personal_trade_blotter.xlsx (Log of personal broker-dealer trades by employees)

## Approach Followed
1. **Restricted Trading Check:** Reconcile the `personal_trade_blotter` against the `wall_cross_register` matching on both `employee_id` and the `ticker_symbol`.
2. **Status Verification:** If an employee executed a trade for a ticker they were wall-crossed on, the `restriction_status` on the register must be 'CLEARED'.
3. **Cool-Off Period Check:** If the status is 'CLEARED', verify that the `trade_date` occurred strictly AFTER the `clearance_date`. Any trades executed prior to clearance are critical violations.