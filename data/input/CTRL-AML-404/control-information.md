# Control ID: CTRL-AML-404

## Risk Statement
Failure to perform Enhanced Due Diligence (EDD) on high-risk customers or onboarding entities listed on the OFAC Sanctions list violates the Bank Secrecy Act (BSA) and USA PATRIOT Act, resulting in severe regulatory fines.

## Source Information
Evidence provided by the Financial Crimes Unit:
- onboarding_log.xlsx (List of all new accounts)
- edd_tracker.xlsx (Log of completed Enhanced Due Diligence reviews)
- ofac_watch_list.xlsx (Current restricted entities)

## Approach Followed
1. **Sanctions Screening:** Reconcile the `onboarding_log` against the `ofac_watch_list` using the customer's `tax_id`. Ensure no onboarded customer exists on the restricted list. If a match is found, it is a critical violation.
2. **EDD SLA Verification:** For all customers in the `onboarding_log` with a `risk_rating` of 'HIGH', the system must verify they exist in the `edd_tracker`.
3. **EDD Temporal Check:** For those high-risk customers, the `edd_completion_date` must occur within 14 days of the `onboarding_date`. If the EDD is missing, or took longer than 14 days, the control fails.