from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def setup_base_dir():
    base = Path("./data/input")
    base.mkdir(parents=True, exist_ok=True)
    return base


def generate_iam_3way(base_dir, num_records=5000):
    print("Generating 3-Way IAM Access Revocation Data...")
    project_dir = base_dir / "project_iam_007"
    project_dir.mkdir(exist_ok=True)

    # 1. HR Terminations
    term_dates = [
        datetime(2025, 12, 1) + timedelta(days=np.random.randint(0, 30))
        for _ in range(num_records)
    ]
    emp_ids = [f"EMP_TERM_{str(i).zfill(5)}" for i in range(1, num_records + 1)]

    df_hr = pd.DataFrame(
        {
            "employee_id": emp_ids,
            "termination_date": [d.strftime("%Y-%m-%d") for d in term_dates],
            "termination_type": np.random.choice(
                ["VOLUNTARY", "INVOLUNTARY", "RETIREMENT"],
                num_records,
                p=[0.6, 0.3, 0.1],
            ),
        }
    )

    # 2. Service Tickets (Most are closed within 1 day)
    closed_dates = [d + timedelta(days=np.random.randint(0, 2)) for d in term_dates]
    df_tickets = pd.DataFrame(
        {
            "ticket_id": [f"TKT_{str(i).zfill(6)}" for i in range(1, num_records + 1)],
            "employee_id": emp_ids,
            "ticket_status": ["CLOSED"] * num_records,
            "closed_date": [d.strftime("%Y-%m-%d") for d in closed_dates],
        }
    )

    # 3. System Accounts (Ideally, terminated users are missing from this file)
    # We create a baseline where NO terminated employees are in the system account export.
    active_emp_ids = [f"EMP_ACTIVE_{str(i).zfill(5)}" for i in range(1, 10000)]
    df_accounts = pd.DataFrame(
        {
            "account_id": [f"ACC_{str(i).zfill(6)}" for i in range(1, 10000)],
            "employee_id": active_emp_ids,
            "last_login": "2026-01-01",
        }
    )

    # INJECT EXCEPTIONS
    # Exception 1: SLA Breach (Ticket took 5 days to close)
    idx_sla = df_hr[df_hr["termination_type"] == "INVOLUNTARY"].index[0]
    breach_term_date = term_dates[idx_sla]
    df_tickets.loc[idx_sla, "closed_date"] = (
        breach_term_date + timedelta(days=5)
    ).strftime("%Y-%m-%d")
    print(
        f" -> IAM Seed 1 (SLA Breach): Ticket closed 5 days late for {emp_ids[idx_sla]}"
    )

    # Exception 2: "The Rubber Stamp" (Ticket is closed on time, but account STILL EXISTS in system)
    idx_ghost = df_hr[df_hr["termination_type"] == "VOLUNTARY"].index[1]
    ghost_emp = emp_ids[idx_ghost]
    # Sneak the terminated employee into the active accounts extract!
    df_accounts.loc[9999] = ["ACC_GHOST_01", ghost_emp, "2026-01-05"]
    print(
        f" -> IAM Seed 2 (Ghost Account): Ticket closed, but {ghost_emp} still exists in active accounts!"
    )

    df_hr.to_excel(project_dir / "hr_terminations.xlsx", index=False)
    df_tickets.to_excel(project_dir / "service_tickets.xlsx", index=False)
    df_accounts.to_excel(project_dir / "system_accounts.xlsx", index=False)


def generate_vrm_interleaved(base_dir, num_records=8000):
    print("Generating Interleaved Vendor Risk Management Data...")
    project_dir = base_dir / "project_vrm_999"
    project_dir.mkdir(exist_ok=True)

    vendor_ids = [f"VND_{str(i).zfill(4)}" for i in range(1, num_records + 1)]

    # 1. Active Contracts
    renew_dates = [
        datetime(2026, 6, 1) + timedelta(days=np.random.randint(0, 180))
        for _ in range(num_records)
    ]
    df_contracts = pd.DataFrame(
        {
            "contract_id": [
                f"CON_{str(i).zfill(5)}" for i in range(1, num_records + 1)
            ],
            "vendor_id": vendor_ids,
            "contract_status": np.random.choice(
                ["ACTIVE", "EXPIRED", "PENDING"], num_records, p=[0.7, 0.2, 0.1]
            ),
            "renewal_date": [d.strftime("%Y-%m-%d") for d in renew_dates],
        }
    )

    # 2. Vendor Master
    df_vendors = pd.DataFrame(
        {
            "vendor_id": vendor_ids,
            "vendor_name": [f"Supplier {i}" for i in range(1, num_records + 1)],
            "criticality": np.random.choice(
                ["TIER_1", "TIER_2", "TIER_3"], num_records, p=[0.15, 0.35, 0.50]
            ),
        }
    )

    # 3. Security Assessments (Only for Tier 1 & 2)
    assessed_vendors = df_vendors[df_vendors["criticality"].isin(["TIER_1", "TIER_2"])][
        "vendor_id"
    ].tolist()

    # Normally, expiration is 1 year after renewal
    exp_dates = {
        vid: (
            datetime.strptime(
                df_contracts[df_contracts["vendor_id"] == vid]["renewal_date"].values[
                    0
                ],
                "%Y-%m-%d",
            )
            + timedelta(days=365)
        ).strftime("%Y-%m-%d")
        for vid in assessed_vendors
    }

    df_security = pd.DataFrame(
        {
            "assessment_id": [
                f"SEC_{str(i).zfill(5)}" for i in range(1, len(assessed_vendors) + 1)
            ],
            "vendor_id": assessed_vendors,
            "assessment_status": ["PASSED"] * len(assessed_vendors),
            "expiration_date": [exp_dates[vid] for vid in assessed_vendors],
        }
    )

    # INJECT EXCEPTIONS
    active_tier1_vids = df_contracts[
        (df_contracts["contract_status"] == "ACTIVE")
        & (
            df_contracts["vendor_id"].isin(
                df_vendors[df_vendors["criticality"] == "TIER_1"]["vendor_id"]
            )
        )
    ]["vendor_id"].tolist()

    # Exception 1: Active Tier 1 Vendor FAILED security assessment
    fail_vid = active_tier1_vids[0]
    df_security.loc[df_security["vendor_id"] == fail_vid, "assessment_status"] = (
        "FAILED"
    )
    print(
        f" -> VRM Seed 1 (Failed Audit): Vendor {fail_vid} is Active and Tier 1, but FAILED security audit."
    )

    # Exception 2: Active Tier 1 Vendor's Security Expires BEFORE the contract renews
    expired_vid = active_tier1_vids[1]
    renewal_dt = datetime.strptime(
        df_contracts[df_contracts["vendor_id"] == expired_vid]["renewal_date"].values[
            0
        ],
        "%Y-%m-%d",
    )
    # Set expiration to 30 days BEFORE renewal
    df_security.loc[df_security["vendor_id"] == expired_vid, "expiration_date"] = (
        renewal_dt - timedelta(days=30)
    ).strftime("%Y-%m-%d")
    print(
        f" -> VRM Seed 2 (Expired Security): Vendor {expired_vid} security expires BEFORE contract renewal."
    )

    df_contracts.to_excel(project_dir / "active_contracts.xlsx", index=False)
    df_vendors.to_excel(project_dir / "vendor_master.xlsx", index=False)
    df_security.to_excel(project_dir / "security_assessments.xlsx", index=False)


if __name__ == "__main__":
    base = setup_base_dir()
    generate_iam_3way(base)
    generate_vrm_interleaved(base)
    print("\nMulti-Step Boundary Tests Successfully Generated!")
