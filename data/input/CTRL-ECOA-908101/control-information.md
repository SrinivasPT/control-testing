# Control ID: CTRL-908101

## Risk Statement
Failure to properly identify and manage risks associated with the Equal Credit Opportunity Act (ECOA) may lead to potential regulatory violations, enforcement actions, financial penalties, and reputational damage.

## Source Information
The Compliance Monitoring team is responsible for requesting evidence of control execution for CTRL-908101 from the control owner. Evidence provided by the frontline may include:
- Excel reports (campaign_reconciliation_report.xlsx)

## Approach Followed
- Examine the evidence to ensure the Campaign System compared the postback file from the IC Group to reconcile letter requests from the prior day with what was printed.
- If an error is identified related to an adverse action letter (reconciliation_status is MISMATCH):
  - Evidence that a business-directed BREF was raised (bref_raised_flag = Y).
  - Evidence that the error was resolved and the letter was sent within 30 days of the application date.
