# Control ID: CTRL-IAM-007

## Risk Statement
Failure to completely revoke Tier 1 application access for terminated employees within 24 hours violates SOX and ISO 27001, posing a critical data exfiltration risk.

## Source Information
- hr_terminations.xlsx (Official HR log of exited employees)
- service_tickets.xlsx (ITSM/ServiceNow logs for access revocation requests)
- system_accounts.xlsx (Raw export of current active accounts from the Tier 1 application)

## Approach Followed
1. Filter the `hr_terminations` file to only evaluate employees where `termination_type` is 'VOLUNTARY' or 'INVOLUNTARY'.
2. Join this population to `service_tickets` matching on `employee_id`.
3. Join the resulting population to `system_accounts` matching on `employee_id`.
4. Verify that the `ticket_status` in the service tickets is 'CLOSED'.
5. Verify that the ticket `closed_date` occurred on or before the `termination_date` plus 1 day.
6. Verify that the employee does NOT exist in the Tier 1 application (i.e., ensure the `system_accounts` employee_id IS NULL to prove the account was actually deleted, regardless of what the ticket says).