# Control ID: CTRL-ITGC-001

## Risk Statement
Retaining active system access for terminated employees or unknown users poses a critical cybersecurity and data exfiltration risk, violating SOX IT General Controls (ITGC).

## Source Information
- active_directory.xlsx (List of all provisioned system accounts)
- hr_master_roster.xlsx (Official HR list of employees)

## Approach Followed
1. Reconcile the `active_directory` against the `hr_master_roster` matching on `employee_id`.
2. Filter the population to only evaluate accounts where the Active Directory `account_status` is 'ACTIVE'.
3. Verify that the matching HR `employment_status` is NOT 'TERMINATED'.
4. Ensure that the HR `employment_status` IS NOT NULL (which would indicate a "Ghost Account" provisioned in AD that does not exist in HR).