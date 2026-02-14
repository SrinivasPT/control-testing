from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def generate_ecoa_project(num_records=5000):
    print("Generating ECOA Adverse Action Test Data...")

    # 1. Setup Directory
    project_dir = Path("./project_ecoa")
    project_dir.mkdir(parents=True, exist_ok=True)

    # 2. Generate Base Dates (July 2025 applications)
    base_app_date = datetime(2025, 7, 1)
    app_dates = [
        base_app_date + timedelta(days=np.random.randint(0, 30))
        for _ in range(num_records)
    ]

    # 3. Generate Core Data
    data = {
        "record_id": [f"REC_{str(i).zfill(6)}" for i in range(1, num_records + 1)],
        "application_id": [f"APP_{str(i).zfill(7)}" for i in range(1, num_records + 1)],
        "application_date": [d.strftime("%Y-%m-%d") for d in app_dates],
        "letter_type": np.random.choice(
            ["Adverse Action", "Welcome Letter", "Credit Increase", "Approval"],
            num_records,
            p=[0.4, 0.3, 0.15, 0.15],
        ),
        "system_requested_count": [1] * num_records,
        "ic_group_printed_count": [1] * num_records,
        "reconciliation_status": ["MATCHED"] * num_records,
        "bref_raised_flag": ["N"] * num_records,
        "resolution_date": [None] * num_records,
    }

    df = pd.DataFrame(data)

    # --- 4. INJECT TEST SCENARIOS ---
    print("Injecting complex conditional ECOA exceptions...")

    # Randomly make some records mismatched (Normal operational noise)
    mismatch_idx = np.random.choice(df.index, size=100, replace=False)
    df.loc[mismatch_idx, "ic_group_printed_count"] = 0
    df.loc[mismatch_idx, "reconciliation_status"] = "MISMATCH"

    # Extract only the adverse action mismatches to seed our specific tests
    aa_mismatches = df[
        (df["reconciliation_status"] == "MISMATCH")
        & (df["letter_type"] == "Adverse Action")
    ].index.tolist()

    # SCENARIO A: Properly handled exception (Should PASS control)
    # Mismatched, BREF raised, resolved within 10 days of app_date
    idx_pass = aa_mismatches[0]
    app_date_pass = datetime.strptime(
        str(df.loc[idx_pass, "application_date"]), "%Y-%m-%d"
    )
    df.loc[idx_pass, "bref_raised_flag"] = "Y"
    df.loc[idx_pass, "resolution_date"] = (app_date_pass + timedelta(days=10)).strftime(
        "%Y-%m-%d"
    )
    print(
        f" -> Properly Handled Exception seeded at Record: {df.loc[idx_pass, 'record_id']} (Passes)"
    )

    # SCENARIO B: Missing BREF (Should FAIL control)
    idx_fail_1 = aa_mismatches[1]
    app_date_fail_1 = datetime.strptime(
        str(df.loc[idx_fail_1, "application_date"]), "%Y-%m-%d"
    )
    df.loc[idx_fail_1, "bref_raised_flag"] = "N"  # Forgot to raise BREF
    df.loc[idx_fail_1, "resolution_date"] = (
        app_date_fail_1 + timedelta(days=10)
    ).strftime("%Y-%m-%d")
    print(
        f" -> Missing BREF Exception seeded at Record: {df.loc[idx_fail_1, 'record_id']} (Fails)"
    )

    # SCENARIO C: 30-Day SLA Breach (Should FAIL control)
    idx_fail_2 = aa_mismatches[2]
    app_date_fail_2 = datetime.strptime(
        str(df.loc[idx_fail_2, "application_date"]), "%Y-%m-%d"
    )
    df.loc[idx_fail_2, "bref_raised_flag"] = "Y"
    # Resolved, but took 45 days (Violates ECOA 30-day rule)
    df.loc[idx_fail_2, "resolution_date"] = (
        app_date_fail_2 + timedelta(days=45)
    ).strftime("%Y-%m-%d")
    print(
        f" -> 30-Day SLA Breach Exception seeded at Record: {df.loc[idx_fail_2, 'record_id']} (Fails)"
    )

    # 5. Save to Excel
    df.to_excel(project_dir / "campaign_reconciliation_report.xlsx", index=False)

    # 6. Save the Control Markdown
    control_md = """# Control ID: CTRL-908101

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
"""
    with open(project_dir / "control-information.md", "w") as f:
        f.write(control_md)

    print(f"\nSuccess! Created 'data/input/project_ecoa' with {num_records} rows.")


if __name__ == "__main__":
    generate_ecoa_project()
