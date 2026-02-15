# Control Testing Review Document

## Project: CTRL-AML-404

### Control Information

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


### Excel File Structures

#### edd_tracker.xlsx

**Columns:** customer_id, edd_completion_date, analyst_id

**Sample Data:**
```
customer_id edd_completion_date analyst_id
CUST_000004          2025-09-10 ANALYST_01
CUST_000033          2025-08-12 ANALYST_01
CUST_000042          2025-09-06 ANALYST_01
```


#### ofac_watch_list.xlsx

**Columns:** tax_id, sanction_entity_name, listed_date

**Sample Data:**
```
     tax_id sanction_entity_name listed_date
TAX_0000501        BAD_ACTOR_LLC  2020-01-01
TAX_0011001        BAD_ACTOR_LLC  2020-01-01
TAX_0011002        BAD_ACTOR_LLC  2020-01-01
```


#### onboarding_log.xlsx

**Columns:** customer_id, tax_id, onboarding_date, risk_rating, account_status

**Sample Data:**
```
customer_id      tax_id onboarding_date risk_rating account_status
CUST_000001 TAX_0000001      2025-08-13         LOW           OPEN
CUST_000002 TAX_0000002      2025-08-06         LOW           OPEN
CUST_000003 TAX_0000003      2025-08-01         LOW           OPEN
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-AML-404",
    "version": "1.0",
    "owner_role": "Financial Crimes Unit",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "Bank Secrecy Act (BSA)",
      "USA PATRIOT Act"
    ],
    "risk_objective": "Ensure Enhanced Due Diligence (EDD) is performed on high-risk customers and no onboarded customers are on the OFAC Sanctions list"
  },
  "ontology_bindings": [
    {
      "business_term": "customer tax identifier",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "tax_id",
      "data_type": "string"
    },
    {
      "business_term": "customer risk rating",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "risk_rating",
      "data_type": "string"
    },
    {
      "business_term": "customer onboarding date",
      "dataset_alias": "onboarding_log_sheet1",
      "technical_field": "onboarding_date",
      "data_type": "date"
    },
    {
      "business_term": "customer identifier",
      "dataset_alias": "edd_tracker_sheet1",
      "technical_field": "customer_id",
      "data_type": "string"
    },
    {
      "business_term": "EDD completion date",
      "dataset_alias": "edd_tracker_sheet1",
      "technical_field": "edd_completion_date",
      "data_type": "date"
    },
    {
      "business_term": "sanctioned entity tax identifier",
      "dataset_alias": "ofac_watch_list_sheet1",
      "technical_field": "tax_id",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "onboarding_log_sheet1",
    "steps": [
      {
        "step_id": "join_ofac_screening",
        "action": {
          "operation": "join_left",
          "left_dataset": "onboarding_log_sheet1",
          "right_dataset": "ofac_watch_list_sheet1",
          "left_keys": [
            "tax_id"
          ],
          "right_keys": [
            "tax_id"
          ]
        }
      },
      {
        "step_id": "join_edd_tracker",
        "action": {
          "operation": "join_left",
          "left_dataset": "onboarding_log_sheet1",
          "right_dataset": "edd_tracker_sheet1",
          "left_keys": [
            "tax_id"
          ],
          "right_keys": [
            "customer_id"
          ]
        }
      },
      {
        "step_id": "filter_high_risk",
        "action": {
          "operation": "filter_comparison",
          "field": "risk_rating",
          "operator": "eq",
          "value": "HIGH"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "No onboarded customer should exist on the OFAC sanctions list",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "right_tax_id",
      "operator": "eq",
      "expected_value": null,
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_2",
      "description": "High-risk customers must have an EDD completion record",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "customer_id",
      "operator": "neq",
      "expected_value": null,
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_3",
      "description": "EDD must be completed within 14 days of onboarding for high-risk customers",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "temporal_date_math",
      "base_date_field": "edd_completion_date",
      "operator": "lte",
      "target_date_field": "onboarding_date",
      "offset_days": 14
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "Financial Crimes Unit"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\onboarding_log_sheet1.parquet')),
join_ofac_screening AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (tax_id),
           right_tbl.tax_id AS right_tax_id
    FROM base
    LEFT JOIN read_parquet('data\parquet\ofac_watch_list_sheet1.parquet') AS right_tbl
    ON base.tax_id = right_tbl.tax_id
),
join_edd_tracker AS (
    SELECT join_ofac_screening.*,
           right_tbl.* EXCLUDE (customer_id),
           right_tbl.customer_id AS right_customer_id
    FROM join_ofac_screening
    LEFT JOIN read_parquet('data\parquet\edd_tracker_sheet1.parquet') AS right_tbl
    ON join_ofac_screening.tax_id = right_tbl.customer_id
)

SELECT *
FROM join_edd_tracker
WHERE (risk_rating = 'HIGH') 
  AND ((right_tax_id IS NULL) IS NOT TRUE OR (customer_id IS NOT NULL) IS NOT TRUE OR (CAST(edd_completion_date AS DATE) <= CAST(onboarding_date AS DATE) + INTERVAL 14 DAY) IS NOT TRUE)
```


## Project: CTRL-CASS-006

### Control Information

# Control ID: CTRL-CASS-006

## Risk Statement
Failure to adequately segregate and fund Client Money accounts violates the FCA CASS rules, risking severe penalties and loss of banking license.

## Source Information
- client_money_ledger.xlsx (Daily snapshot of client deposits and firm funding)

## Approach Followed
1. Filter the ledger to only evaluate rows where the `account_type` is 'CLIENT_FUNDS'.
2. Verify that the aggregated SUM of the `current_balance` grouped by `calculation_date` is strictly greater than or equal to $50,000,000 (The firm's mandatory minimum segregated buffer).


### Excel File Structures

#### client_money_ledger.xlsx

**Columns:** calculation_date, account_id, account_type, current_balance

**Sample Data:**
```
calculation_date account_id account_type  current_balance
      2025-11-01       CM_0 CLIENT_FUNDS    663218.496178
      2025-11-01       CM_1 CLIENT_FUNDS    634852.243725
      2025-11-01       CM_2 CLIENT_FUNDS    725034.797185
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-CASS-006",
    "version": "1.0",
    "owner_role": "CASS Compliance Officer",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "FCA CASS Rules"
    ],
    "risk_objective": "Ensure adequate segregation and funding of Client Money accounts to avoid regulatory penalties and license loss"
  },
  "ontology_bindings": [
    {
      "business_term": "calculation_date",
      "dataset_alias": "client_money_ledger_sheet1",
      "technical_field": "calculation_date",
      "data_type": "date"
    },
    {
      "business_term": "account_type",
      "dataset_alias": "client_money_ledger_sheet1",
      "technical_field": "account_type",
      "data_type": "string"
    },
    {
      "business_term": "current_balance",
      "dataset_alias": "client_money_ledger_sheet1",
      "technical_field": "current_balance",
      "data_type": "numeric"
    }
  ],
  "population": {
    "base_dataset": "client_money_ledger_sheet1",
    "steps": [
      {
        "step_id": "filter_client_funds",
        "action": {
          "operation": "filter_comparison",
          "field": "account_type",
          "operator": "eq",
          "value": "CLIENT_FUNDS"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "agg_min_segregated_buffer",
      "description": "Verify that the aggregated SUM of current_balance grouped by calculation_date is >= $50,000,000 (mandatory minimum segregated buffer)",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "aggregation",
      "group_by_fields": [
        "calculation_date"
      ],
      "metric_field": "current_balance",
      "aggregation_function": "SUM",
      "operator": "gte",
      "threshold": 50000000.0
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "CASS_Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\client_money_ledger_sheet1.parquet'))

SELECT calculation_date, 
       COUNT(*) as exception_count,
       SUM(current_balance) as total_amount
FROM base
WHERE account_type = 'CLIENT_FUNDS'
GROUP BY calculation_date
HAVING (SUM(current_balance) >= 50000000.0) IS NOT TRUE
```


## Project: CTRL-ECOA-908101

### Control Information

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



### Excel File Structures

#### campaign_reconciliation_report.xlsx

**Columns:** record_id, application_id, application_date, letter_type, system_requested_count, ic_group_printed_count, reconciliation_status, bref_raised_flag, resolution_date

**Sample Data:**
```
 record_id application_id application_date    letter_type  system_requested_count  ic_group_printed_count reconciliation_status bref_raised_flag resolution_date
REC_000001    APP_0000001       2025-07-19       Approval                       1                       1               MATCHED                N             NaN
REC_000002    APP_0000002       2025-07-30 Adverse Action                       1                       1               MATCHED                N             NaN
REC_000003    APP_0000003       2025-07-02       Approval                       1                       1               MATCHED                N             NaN
```


### Generated DSL
No DSL found in database.


### Compiled SQL
No executions found in database.


## Project: CTRL-IAM-007

### Control Information

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


### Excel File Structures

#### hr_terminations.xlsx

**Columns:** employee_id, termination_date, termination_type

**Sample Data:**
```
   employee_id termination_date termination_type
EMP_TERM_00001       2025-12-12        VOLUNTARY
EMP_TERM_00002       2025-12-30        VOLUNTARY
EMP_TERM_00003       2025-12-19        VOLUNTARY
```


#### service_tickets.xlsx

**Columns:** ticket_id, employee_id, ticket_status, closed_date

**Sample Data:**
```
 ticket_id    employee_id ticket_status closed_date
TKT_000001 EMP_TERM_00001        CLOSED  2025-12-12
TKT_000002 EMP_TERM_00002        CLOSED  2025-12-30
TKT_000003 EMP_TERM_00003        CLOSED  2025-12-19
```


#### system_accounts.xlsx

**Columns:** account_id, employee_id, last_login

**Sample Data:**
```
account_id      employee_id last_login
ACC_000001 EMP_ACTIVE_00001 2026-01-01
ACC_000002 EMP_ACTIVE_00002 2026-01-01
ACC_000003 EMP_ACTIVE_00003 2026-01-01
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-IAM-007",
    "version": "1.0",
    "owner_role": "IAM Compliance Officer",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SOX",
      "ISO 27001"
    ],
    "risk_objective": "Prevent data exfiltration by ensuring timely revocation of Tier 1 application access for terminated employees"
  },
  "ontology_bindings": [
    {
      "business_term": "Terminated Employee",
      "dataset_alias": "hr_terminations_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Termination Date",
      "dataset_alias": "hr_terminations_sheet1",
      "technical_field": "termination_date",
      "data_type": "date"
    },
    {
      "business_term": "Termination Type",
      "dataset_alias": "hr_terminations_sheet1",
      "technical_field": "termination_type",
      "data_type": "string"
    },
    {
      "business_term": "Access Revocation Ticket Status",
      "dataset_alias": "service_tickets_sheet1",
      "technical_field": "ticket_status",
      "data_type": "string"
    },
    {
      "business_term": "Ticket Closed Date",
      "dataset_alias": "service_tickets_sheet1",
      "technical_field": "closed_date",
      "data_type": "date"
    },
    {
      "business_term": "Active System Account",
      "dataset_alias": "system_accounts_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "hr_terminations_sheet1",
    "steps": [
      {
        "step_id": "filter_termination_types",
        "action": {
          "operation": "filter_in_list",
          "field": "termination_type",
          "values": [
            "VOLUNTARY",
            "INVOLUNTARY"
          ]
        }
      },
      {
        "step_id": "join_service_tickets",
        "action": {
          "operation": "join_left",
          "left_dataset": "hr_terminations_sheet1",
          "right_dataset": "service_tickets_sheet1",
          "left_keys": [
            "employee_id"
          ],
          "right_keys": [
            "employee_id"
          ]
        }
      },
      {
        "step_id": "join_system_accounts",
        "action": {
          "operation": "join_left",
          "left_dataset": "hr_terminations_sheet1",
          "right_dataset": "system_accounts_sheet1",
          "left_keys": [
            "employee_id"
          ],
          "right_keys": [
            "employee_id"
          ]
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "Verify that the ticket status is 'CLOSED'",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "ticket_status",
      "operator": "eq",
      "expected_value": "CLOSED",
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_2",
      "description": "Verify that the ticket closed_date occurred on or before termination_date plus 1 day",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "temporal_date_math",
      "base_date_field": "closed_date",
      "operator": "lte",
      "target_date_field": "termination_date",
      "offset_days": 1
    },
    {
      "assertion_id": "assertion_3",
      "description": "Verify that the employee does NOT exist in the Tier 1 application (right_employee_id IS NULL)",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "right_employee_id",
      "operator": "eq",
      "expected_value": null,
      "ignore_case_and_space": true
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "IAM Access Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\hr_terminations_sheet1.parquet')),
join_service_tickets AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (employee_id),
           right_tbl.employee_id AS right_employee_id
    FROM base
    LEFT JOIN read_parquet('data\parquet\service_tickets_sheet1.parquet') AS right_tbl
    ON base.employee_id = right_tbl.employee_id
),
join_system_accounts AS (
    SELECT join_service_tickets.*,
           right_tbl.* EXCLUDE (employee_id),
           right_tbl.employee_id AS right_employee_id
    FROM join_service_tickets
    LEFT JOIN read_parquet('data\parquet\system_accounts_sheet1.parquet') AS right_tbl
    ON join_service_tickets.employee_id = right_tbl.employee_id
)

SELECT *
FROM join_system_accounts
WHERE (termination_type IN ('VOLUNTARY', 'INVOLUNTARY')) 
  AND ((TRIM(UPPER(CAST(ticket_status AS VARCHAR))) = TRIM(UPPER('CLOSED'))) IS NOT TRUE OR (CAST(closed_date AS DATE) <= CAST(termination_date AS DATE) + INTERVAL 1 DAY) IS NOT TRUE OR (right_employee_id IS NULL) IS NOT TRUE)
```


## Project: CTRL-ITGC-001

### Control Information

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


### Excel File Structures

#### active_directory.xlsx

**Columns:** system_account_id, employee_id, account_status

**Sample Data:**
```
system_account_id employee_id account_status
         AD_00001   EMP_00001         ACTIVE
         AD_00002   EMP_00002         ACTIVE
         AD_00003   EMP_00003         ACTIVE
```


#### hr_master_roster.xlsx

**Columns:** employee_id, employment_status

**Sample Data:**
```
employee_id employment_status
  EMP_00001            ACTIVE
  EMP_00002            ACTIVE
  EMP_00003            ACTIVE
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-ITGC-001",
    "version": "1.0",
    "owner_role": "IT Security Manager",
    "testing_frequency": "Continuous",
    "regulatory_citations": [
      "SOX IT General Controls"
    ],
    "risk_objective": "Prevent unauthorized system access by terminated employees or unknown users"
  },
  "ontology_bindings": [
    {
      "business_term": "Employee ID",
      "dataset_alias": "active_directory_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Account Status",
      "dataset_alias": "active_directory_sheet1",
      "technical_field": "account_status",
      "data_type": "string"
    },
    {
      "business_term": "Employment Status",
      "dataset_alias": "hr_master_roster_sheet1",
      "technical_field": "employment_status",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "active_directory_sheet1",
    "steps": [
      {
        "step_id": "join_hr_data",
        "action": {
          "operation": "join_left",
          "left_dataset": "active_directory_sheet1",
          "right_dataset": "hr_master_roster_sheet1",
          "left_keys": [
            "employee_id"
          ],
          "right_keys": [
            "employee_id"
          ]
        }
      },
      {
        "step_id": "filter_active_accounts",
        "action": {
          "operation": "filter_comparison",
          "field": "account_status",
          "operator": "eq",
          "value": "ACTIVE"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "Verify HR employment status is not 'TERMINATED' for active AD accounts",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "employment_status",
      "operator": "neq",
      "expected_value": "TERMINATED",
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_2",
      "description": "Verify HR employment status is not NULL (no ghost accounts)",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "employment_status",
      "operator": "neq",
      "expected_value": null,
      "ignore_case_and_space": true
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "IT Security Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\active_directory_sheet1.parquet')),
join_hr_data AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (employee_id),
           right_tbl.employee_id AS right_employee_id
    FROM base
    LEFT JOIN read_parquet('data\parquet\hr_master_roster_sheet1.parquet') AS right_tbl
    ON base.employee_id = right_tbl.employee_id
)

SELECT *
FROM join_hr_data
WHERE (account_status = 'ACTIVE') 
  AND ((TRIM(UPPER(CAST(employment_status AS VARCHAR))) != TRIM(UPPER('TERMINATED'))) IS NOT TRUE OR (employment_status IS NOT NULL) IS NOT TRUE)
```


## Project: CTRL-MNPI-707

### Control Information

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


### Excel File Structures

#### personal_trade_blotter.xlsx

**Columns:** trade_id, employee_id, ticker_symbol, trade_date, action

**Sample Data:**
```
  trade_id employee_id ticker_symbol trade_date action
TXN_000001    EMP_0082          TSLA 2025-09-14    BUY
TXN_000002    EMP_0328          TSLA 2025-09-08    BUY
TXN_000003    EMP_0359          MSFT 2025-09-07    BUY
```


#### wall_cross_register.xlsx

**Columns:** employee_id, ticker_symbol, cross_date, restriction_status, clearance_date

**Sample Data:**
```
employee_id ticker_symbol cross_date restriction_status clearance_date
   EMP_0241         GOOGL 2025-09-01         RESTRICTED     2025-09-15
   EMP_0235          NVDA 2025-09-01         RESTRICTED     2025-09-15
   EMP_0155         GOOGL 2025-09-01         RESTRICTED     2025-09-15
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-MNPI-707",
    "version": "1.0",
    "owner_role": "Compliance Officer",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SEC Rule 10b-5"
    ],
    "risk_objective": "Prevent insider trading by ensuring employees do not execute personal trades on restricted tickers while exposed to MNPI."
  },
  "ontology_bindings": [
    {
      "business_term": "Employee ID",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Ticker Symbol",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "ticker_symbol",
      "data_type": "string"
    },
    {
      "business_term": "Trade Date",
      "dataset_alias": "personal_trade_blotter_sheet1",
      "technical_field": "trade_date",
      "data_type": "date"
    },
    {
      "business_term": "Employee ID",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "employee_id",
      "data_type": "string"
    },
    {
      "business_term": "Ticker Symbol",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "ticker_symbol",
      "data_type": "string"
    },
    {
      "business_term": "Restriction Status",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "restriction_status",
      "data_type": "string"
    },
    {
      "business_term": "Clearance Date",
      "dataset_alias": "wall_cross_register_sheet1",
      "technical_field": "clearance_date",
      "data_type": "date"
    }
  ],
  "population": {
    "base_dataset": "personal_trade_blotter_sheet1",
    "steps": [
      {
        "step_id": "join_wall_cross_register",
        "action": {
          "operation": "join_left",
          "left_dataset": "personal_trade_blotter_sheet1",
          "right_dataset": "wall_cross_register_sheet1",
          "left_keys": [
            "employee_id",
            "ticker_symbol"
          ],
          "right_keys": [
            "employee_id",
            "ticker_symbol"
          ]
        }
      },
      {
        "step_id": "filter_matched_records",
        "action": {
          "operation": "filter_is_null",
          "field": "right_employee_id",
          "is_null": false
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "Check that restriction_status is 'CLEARED' for matched trades",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "restriction_status",
      "operator": "eq",
      "expected_value": "CLEARED",
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_2",
      "description": "Check that trade_date is strictly after clearance_date",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "column_comparison",
      "left_field": "trade_date",
      "operator": "gt",
      "right_field": "clearance_date"
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "MNPI_Violations_Queue"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\personal_trade_blotter_sheet1.parquet')),
join_wall_cross_register AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (employee_id, ticker_symbol),
           right_tbl.employee_id AS right_employee_id, right_tbl.ticker_symbol AS right_ticker_symbol
    FROM base
    LEFT JOIN read_parquet('data\parquet\wall_cross_register_sheet1.parquet') AS right_tbl
    ON base.employee_id = right_tbl.employee_id AND base.ticker_symbol = right_tbl.ticker_symbol
)

SELECT *
FROM join_wall_cross_register
WHERE (right_employee_id IS NOT NULL) 
  AND ((TRIM(UPPER(CAST(restriction_status AS VARCHAR))) = TRIM(UPPER('CLEARED'))) IS NOT TRUE OR (trade_date > clearance_date) IS NOT TRUE)
```


## Project: CTRL-OPS-T2-003

### Control Information

# Control ID: CTRL-OPS-T2-003

## Risk Statement
Failure to settle equity trades within the standard T+2 (Trade Date + 2 days) window exposes the firm to counterparty credit risk and regulatory fines from the SEC/FINRA.

## Source Information
- equity_settlements.xlsx (Log of all executed and settled trades)

## Approach Followed
1. Filter the population to trades where the `trade_status` is 'SETTLED'.
2. Verify that the `settlement_date` occurred on or before the `trade_date` plus 2 days.


### Excel File Structures

#### equity_settlements.xlsx

**Columns:** trade_id, trade_date, settlement_date, trade_status

**Sample Data:**
```
 trade_id trade_date settlement_date trade_status
EQ_000001 2025-10-07      2025-10-09      SETTLED
EQ_000002 2025-10-18      2025-10-19      SETTLED
EQ_000003 2025-10-08      2025-10-09      SETTLED
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-OPS-T2-003",
    "version": "1.0",
    "owner_role": "Operations",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SEC",
      "FINRA"
    ],
    "risk_objective": "Mitigate counterparty credit risk and regulatory fines by ensuring equity trades settle within T+2 window"
  },
  "ontology_bindings": [
    {
      "business_term": "trade_status",
      "dataset_alias": "equity_settlements_sheet1",
      "technical_field": "trade_status",
      "data_type": "string"
    },
    {
      "business_term": "trade_date",
      "dataset_alias": "equity_settlements_sheet1",
      "technical_field": "trade_date",
      "data_type": "date"
    },
    {
      "business_term": "settlement_date",
      "dataset_alias": "equity_settlements_sheet1",
      "technical_field": "settlement_date",
      "data_type": "date"
    }
  ],
  "population": {
    "base_dataset": "equity_settlements_sheet1",
    "steps": [
      {
        "step_id": "filter_settled_trades",
        "action": {
          "operation": "filter_comparison",
          "field": "trade_status",
          "operator": "eq",
          "value": "SETTLED"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assert_t2_settlement",
      "description": "Verify settlement_date occurs on or before trade_date plus 2 days",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "temporal_date_math",
      "base_date_field": "settlement_date",
      "operator": "lte",
      "target_date_field": "trade_date",
      "offset_days": 2
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Auto-Close_If_Pass",
    "exception_routing_queue": "Operations_Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\equity_settlements_sheet1.parquet'))

SELECT *
FROM base
WHERE (trade_status = 'SETTLED') 
  AND ((CAST(settlement_date AS DATE) <= CAST(trade_date AS DATE) + INTERVAL 2 DAY) IS NOT TRUE)
```


## Project: CTRL-SOX-AP-004

### Control Information

# Control ID: CTRL-SOX-AP-004

## Risk Statement
Unauthorized approval of high-value invoices circumvents the firm's Delegation of Authority matrix, risking financial fraud and SOX 404 violations.

## Source Information
- ap_invoices.xlsx (Accounts Payable invoice ledger)
- employee_titles.xlsx (HR mapping of employees to their corporate titles)

## Approach Followed
1. Filter the `ap_invoices` population to only include invoices where the `invoice_amount` is strictly greater than $100,000.
2. Join to `employee_titles` on `approver_id`.
3. Verify that the `approver_title` of the person who approved the invoice is strictly in the allowed executive list: ['SVP', 'EVP', 'CEO', 'CFO'].


### Excel File Structures

#### ap_invoices.xlsx

**Columns:** invoice_id, invoice_amount, approver_id

**Sample Data:**
```
invoice_id  invoice_amount approver_id
INV_000001         4794.77      APP_52
INV_000002       243862.38      APP_52
INV_000003        36005.03      APP_52
```


#### employee_titles.xlsx

**Columns:** approver_id, approver_title

**Sample Data:**
```
approver_id approver_title
      APP_1            CEO
      APP_2            CFO
      APP_3             VP
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-SOX-AP-004",
    "version": "1.0",
    "owner_role": "Accounts Payable Manager",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SOX 404"
    ],
    "risk_objective": "Prevent unauthorized approval of high-value invoices to avoid financial fraud and SOX violations"
  },
  "ontology_bindings": [
    {
      "business_term": "invoice_amount",
      "dataset_alias": "ap_invoices_sheet1",
      "technical_field": "invoice_amount",
      "data_type": "numeric"
    },
    {
      "business_term": "approver_id",
      "dataset_alias": "ap_invoices_sheet1",
      "technical_field": "approver_id",
      "data_type": "string"
    },
    {
      "business_term": "approver_title",
      "dataset_alias": "employee_titles_sheet1",
      "technical_field": "approver_title",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "ap_invoices_sheet1",
    "steps": [
      {
        "step_id": "filter_high_value_invoices",
        "action": {
          "operation": "filter_comparison",
          "field": "invoice_amount",
          "operator": "gt",
          "value": 100000
        }
      },
      {
        "step_id": "join_employee_titles",
        "action": {
          "operation": "join_left",
          "left_dataset": "ap_invoices_sheet1",
          "right_dataset": "employee_titles_sheet1",
          "left_keys": [
            "approver_id"
          ],
          "right_keys": [
            "approver_id"
          ]
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assert_approver_title_allowed",
      "description": "Verify approver title is in allowed executive list",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "approver_title",
      "operator": "in",
      "expected_value": [
        "SVP",
        "EVP",
        "CEO",
        "CFO"
      ],
      "ignore_case_and_space": true
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "SOX_Exceptions_Queue"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\ap_invoices_sheet1.parquet')),
join_employee_titles AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (approver_id),
           right_tbl.approver_id AS right_approver_id
    FROM base
    LEFT JOIN read_parquet('data\parquet\employee_titles_sheet1.parquet') AS right_tbl
    ON base.approver_id = right_tbl.approver_id
)

SELECT *
FROM join_employee_titles
WHERE (invoice_amount > 100000) 
  AND ((approver_title IN ('SVP', 'EVP', 'CEO', 'CFO')) IS NOT TRUE)
```


## Project: CTRL-TRD-WASH-005

### Control Information

# Control ID: CTRL-TRD-WASH-005

## Risk Statement
Executing a transaction where the buyer and seller are the same entity constitutes Wash Trading, a form of market manipulation prohibited by the SEC and CFTC.

## Source Information
- daily_execution_blotter.xlsx (Log of all market executions)

## Approach Followed
1. Filter the blotter to executed trades (status = 'EXECUTED').
2. Compare the `buyer_account_id` to the `seller_account_id` on every trade.
3. Verify that the `buyer_account_id` does NOT equal the `seller_account_id`.


### Excel File Structures

#### daily_execution_blotter.xlsx

**Columns:** execution_id, status, buyer_account_id, seller_account_id

**Sample Data:**
```
execution_id   status buyer_account_id seller_account_id
EXEC_0000001 EXECUTED         ACC_0844          ACC_0081
EXEC_0000002 EXECUTED         ACC_0548          ACC_0502
EXEC_0000003 EXECUTED         ACC_0281          ACC_0448
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-TRD-WASH-005",
    "version": "1.0",
    "owner_role": "Compliance Officer",
    "testing_frequency": "Daily",
    "regulatory_citations": [
      "SEC Rule 10b-5",
      "CFTC Regulation 180.1"
    ],
    "risk_objective": "Prevent wash trading by ensuring buyer and seller are not the same entity"
  },
  "ontology_bindings": [
    {
      "business_term": "trade_status",
      "dataset_alias": "daily_execution_blotter_sheet1",
      "technical_field": "status",
      "data_type": "string"
    },
    {
      "business_term": "buyer_account",
      "dataset_alias": "daily_execution_blotter_sheet1",
      "technical_field": "buyer_account_id",
      "data_type": "string"
    },
    {
      "business_term": "seller_account",
      "dataset_alias": "daily_execution_blotter_sheet1",
      "technical_field": "seller_account_id",
      "data_type": "string"
    }
  ],
  "population": {
    "base_dataset": "daily_execution_blotter_sheet1",
    "steps": [
      {
        "step_id": "filter_executed_trades",
        "action": {
          "operation": "filter_comparison",
          "field": "status",
          "operator": "eq",
          "value": "EXECUTED"
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "wash_trading_check",
      "description": "Verify buyer_account_id does not equal seller_account_id to prevent wash trading",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "column_comparison",
      "left_field": "buyer_account_id",
      "operator": "neq",
      "right_field": "seller_account_id"
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "Market_Manipulation_Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\daily_execution_blotter_sheet1.parquet'))

SELECT *
FROM base
WHERE (status = 'EXECUTED') 
  AND ((buyer_account_id != seller_account_id) IS NOT TRUE)
```


## Project: CTRL-VRM-999

### Control Information

# Control ID: CTRL-VRM-999

## Risk Statement
Utilizing critical third-party vendors who have failed security assessments or possess expired SOC2 reports violates OCC and GDPR third-party risk management frameworks.

## Source Information
- active_contracts.xlsx (Procurement log of active vendor engagements)
- vendor_master.xlsx (Metadata defining vendor criticality)
- security_assessments.xlsx (Log of vendor cybersecurity audits)

## Approach Followed
1. Filter `active_contracts` to engagements where `contract_status` is 'ACTIVE'.
2. Join the population to `vendor_master` matching on `vendor_id`.
3. Filter the population again to only include vendors where `criticality` is 'TIER_1'.
4. Join the resulting high-risk population to `security_assessments` matching on `vendor_id`.
5. Verify that the most recent `assessment_status` is 'PASSED'.
6. Verify that the security `expiration_date` is strictly AFTER the contract `renewal_date` (ensuring the vendor's security clearance outlasts the current contract term).


### Excel File Structures

#### active_contracts.xlsx

**Columns:** contract_id, vendor_id, contract_status, renewal_date

**Sample Data:**
```
contract_id vendor_id contract_status renewal_date
  CON_00001  VND_0001          ACTIVE   2026-09-07
  CON_00002  VND_0002          ACTIVE   2026-10-20
  CON_00003  VND_0003          ACTIVE   2026-09-19
```


#### security_assessments.xlsx

**Columns:** assessment_id, vendor_id, assessment_status, expiration_date

**Sample Data:**
```
assessment_id vendor_id assessment_status expiration_date
    SEC_00001  VND_0003            PASSED      2027-09-19
    SEC_00002  VND_0006            PASSED      2027-06-09
    SEC_00003  VND_0011            PASSED      2027-09-03
```


#### vendor_master.xlsx

**Columns:** vendor_id, vendor_name, criticality

**Sample Data:**
```
vendor_id vendor_name criticality
 VND_0001  Supplier 1      TIER_3
 VND_0002  Supplier 2      TIER_3
 VND_0003  Supplier 3      TIER_2
```


### Generated DSL

```json
{
  "governance": {
    "control_id": "CTRL-VRM-999",
    "version": "1.0",
    "owner_role": "Third-Party Risk Management",
    "testing_frequency": "Quarterly",
    "regulatory_citations": [
      "OCC",
      "GDPR"
    ],
    "risk_objective": "Prevent utilization of critical third-party vendors with failed security assessments or expired SOC2 reports"
  },
  "ontology_bindings": [
    {
      "business_term": "vendor_id",
      "dataset_alias": "active_contracts_sheet1",
      "technical_field": "vendor_id",
      "data_type": "string"
    },
    {
      "business_term": "contract_status",
      "dataset_alias": "active_contracts_sheet1",
      "technical_field": "contract_status",
      "data_type": "string"
    },
    {
      "business_term": "renewal_date",
      "dataset_alias": "active_contracts_sheet1",
      "technical_field": "renewal_date",
      "data_type": "date"
    },
    {
      "business_term": "criticality",
      "dataset_alias": "vendor_master_sheet1",
      "technical_field": "criticality",
      "data_type": "string"
    },
    {
      "business_term": "assessment_status",
      "dataset_alias": "security_assessments_sheet1",
      "technical_field": "assessment_status",
      "data_type": "string"
    },
    {
      "business_term": "expiration_date",
      "dataset_alias": "security_assessments_sheet1",
      "technical_field": "expiration_date",
      "data_type": "date"
    }
  ],
  "population": {
    "base_dataset": "active_contracts_sheet1",
    "steps": [
      {
        "step_id": "filter_active_contracts",
        "action": {
          "operation": "filter_comparison",
          "field": "contract_status",
          "operator": "eq",
          "value": "ACTIVE"
        }
      },
      {
        "step_id": "join_vendor_master",
        "action": {
          "operation": "join_left",
          "left_dataset": "active_contracts_sheet1",
          "right_dataset": "vendor_master_sheet1",
          "left_keys": [
            "vendor_id"
          ],
          "right_keys": [
            "vendor_id"
          ]
        }
      },
      {
        "step_id": "filter_tier1_vendors",
        "action": {
          "operation": "filter_comparison",
          "field": "criticality",
          "operator": "eq",
          "value": "TIER_1"
        }
      },
      {
        "step_id": "join_security_assessments",
        "action": {
          "operation": "join_left",
          "left_dataset": "active_contracts_sheet1",
          "right_dataset": "security_assessments_sheet1",
          "left_keys": [
            "vendor_id"
          ],
          "right_keys": [
            "vendor_id"
          ]
        }
      }
    ],
    "sampling": null
  },
  "assertions": [
    {
      "assertion_id": "assertion_1",
      "description": "Verify that the most recent assessment_status is 'PASSED'",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "value_match",
      "field": "assessment_status",
      "operator": "eq",
      "expected_value": "PASSED",
      "ignore_case_and_space": true
    },
    {
      "assertion_id": "assertion_2",
      "description": "Verify that the security expiration_date is strictly AFTER the contract renewal_date",
      "materiality_threshold_percent": 0.0,
      "assertion_type": "column_comparison",
      "left_field": "expiration_date",
      "operator": "gt",
      "right_field": "renewal_date"
    }
  ],
  "evidence": {
    "retention_years": 7,
    "reviewer_workflow": "Requires_Human_Signoff",
    "exception_routing_queue": "Third-Party Risk Exceptions"
  }
}
```


### Compiled SQL

#### Execution 1

```sql
WITH base AS (SELECT * FROM read_parquet('data\parquet\active_contracts_sheet1.parquet')),
join_vendor_master AS (
    SELECT base.*,
           right_tbl.* EXCLUDE (vendor_id),
           right_tbl.vendor_id AS right_vendor_id
    FROM base
    LEFT JOIN read_parquet('data\parquet\vendor_master_sheet1.parquet') AS right_tbl
    ON base.vendor_id = right_tbl.vendor_id
),
join_security_assessments AS (
    SELECT join_vendor_master.*,
           right_tbl.* EXCLUDE (vendor_id),
           right_tbl.vendor_id AS right_vendor_id
    FROM join_vendor_master
    LEFT JOIN read_parquet('data\parquet\security_assessments_sheet1.parquet') AS right_tbl
    ON join_vendor_master.vendor_id = right_tbl.vendor_id
)

SELECT *
FROM join_security_assessments
WHERE (contract_status = 'ACTIVE' AND criticality = 'TIER_1') 
  AND ((TRIM(UPPER(CAST(assessment_status AS VARCHAR))) = TRIM(UPPER('PASSED'))) IS NOT TRUE OR (expiration_date > renewal_date) IS NOT TRUE)
```

