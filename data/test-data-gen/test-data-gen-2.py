from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def setup_base_dir():
    base = Path("./data/input")
    base.mkdir(parents=True, exist_ok=True)
    return base


def generate_itgc(base_dir, num_records=15000):
    print("Generating ITGC Orphaned Accounts Data...")
    project_dir = base_dir / "project_itgc_001"
    project_dir.mkdir(exist_ok=True)

    # HR Master
    emp_ids = [f"EMP_{str(i).zfill(5)}" for i in range(1, num_records + 1)]
    hr_data = {
        "employee_id": emp_ids,
        "employment_status": np.random.choice(
            ["ACTIVE", "ON_LEAVE", "TERMINATED"], num_records, p=[0.85, 0.05, 0.10]
        ),
    }
    df_hr = pd.DataFrame(hr_data)

    # Active Directory
    ad_data = {
        "system_account_id": [
            f"AD_{str(i).zfill(5)}" for i in range(1, num_records + 1)
        ],
        "employee_id": emp_ids,
        "account_status": np.random.choice(
            ["ACTIVE", "DISABLED"], num_records, p=[0.9, 0.1]
        ),
    }
    df_ad = pd.DataFrame(ad_data)

    # INJECT EXCEPTIONS
    # 1. Terminated Employee still has ACTIVE AD account
    term_idx = df_hr[df_hr["employment_status"] == "TERMINATED"].index[0]
    df_ad.loc[term_idx, "account_status"] = "ACTIVE"
    print(f" -> ITGC Seed 1 (Terminated but Active): {emp_ids[term_idx]}")

    # 2. Ghost Account (In AD, completely missing from HR)
    df_ad.loc[num_records] = ["AD_GHOST", "EMP_99999", "ACTIVE"]
    print(" -> ITGC Seed 2 (Ghost Account): EMP_99999")

    df_hr.to_excel(project_dir / "hr_master_roster.xlsx", index=False)
    df_ad.to_excel(project_dir / "active_directory.xlsx", index=False)


def generate_ops_t2(base_dir, num_records=20000):
    print("Generating T+2 Settlement SLA Data...")
    project_dir = base_dir / "project_ops_003"
    project_dir.mkdir(exist_ok=True)

    trade_dates = [
        datetime(2025, 10, 1) + timedelta(days=np.random.randint(0, 30))
        for _ in range(num_records)
    ]
    # 99% settle in 1 or 2 days
    settle_dates = [d + timedelta(days=np.random.randint(1, 3)) for d in trade_dates]

    df = pd.DataFrame(
        {
            "trade_id": [f"EQ_{str(i).zfill(6)}" for i in range(1, num_records + 1)],
            "trade_date": [d.strftime("%Y-%m-%d") for d in trade_dates],
            "settlement_date": [d.strftime("%Y-%m-%d") for d in settle_dates],
            "trade_status": ["SETTLED"] * num_records,
        }
    )

    # INJECT EXCEPTION: T+5 Settlement Breach
    breach_idx = 5000
    df.loc[breach_idx, "settlement_date"] = (
        trade_dates[breach_idx] + timedelta(days=5)
    ).strftime("%Y-%m-%d")
    print(f" -> OPS Seed 1 (T+5 Breach): {df.loc[breach_idx, 'trade_id']}")

    df.to_excel(project_dir / "equity_settlements.xlsx", index=False)


def generate_sox_ap(base_dir, num_records=10000):
    print("Generating SOX AP Delegation Data...")
    project_dir = base_dir / "project_sox_004"
    project_dir.mkdir(exist_ok=True)

    approver_ids = [f"APP_{i}" for i in range(1, 101)]
    titles = ["ANALYST", "ASSOCIATE", "VP", "SVP", "EVP", "CEO", "CFO"]

    df_titles = pd.DataFrame(
        {"approver_id": approver_ids, "approver_title": np.random.choice(titles, 100)}
    )

    df_inv = pd.DataFrame(
        {
            "invoice_id": [f"INV_{str(i).zfill(6)}" for i in range(1, num_records + 1)],
            "invoice_amount": np.round(np.random.uniform(1000, 250000, num_records), 2),
            "approver_id": np.random.choice(approver_ids, num_records),
        }
    )

    # INJECT EXCEPTION: Analyst approving a $150k invoice
    analyst_id = df_titles[df_titles["approver_title"] == "ANALYST"][
        "approver_id"
    ].iloc[0]
    df_inv.loc[100, "invoice_amount"] = 150000.00
    df_inv.loc[100, "approver_id"] = analyst_id
    print(
        f" -> SOX Seed 1 (Unauthorized Approval): INV_000101 by {analyst_id} (ANALYST)"
    )

    df_titles.to_excel(project_dir / "employee_titles.xlsx", index=False)
    df_inv.to_excel(project_dir / "ap_invoices.xlsx", index=False)


def generate_wash_trade(base_dir, num_records=25000):
    print("Generating Wash Trading Data...")
    project_dir = base_dir / "project_wash_005"
    project_dir.mkdir(exist_ok=True)

    accounts = [f"ACC_{str(i).zfill(4)}" for i in range(1, 1000)]

    df = pd.DataFrame(
        {
            "execution_id": [
                f"EXEC_{str(i).zfill(7)}" for i in range(1, num_records + 1)
            ],
            "status": ["EXECUTED"] * num_records,
            "buyer_account_id": np.random.choice(accounts, num_records),
            "seller_account_id": np.random.choice(accounts, num_records),
        }
    )

    # Ensure no accidental wash trades in baseline
    df.loc[df["buyer_account_id"] == df["seller_account_id"], "seller_account_id"] = (
        "ACC_9999"
    )

    # INJECT EXCEPTION: Wash Trade
    df.loc[8888, "buyer_account_id"] = "ACC_0042"
    df.loc[8888, "seller_account_id"] = "ACC_0042"
    print(" -> WASH Seed 1 (Wash Trade): EXEC_0008889 on ACC_0042")

    df.to_excel(project_dir / "daily_execution_blotter.xlsx", index=False)


def generate_cass(base_dir, num_records=5000):
    print("Generating CASS Client Money Data...")
    project_dir = base_dir / "project_cass_006"
    project_dir.mkdir(exist_ok=True)

    dates = pd.date_range(start="2025-11-01", periods=30, freq="D")

    # Generate daily balances that safely sum over 50M
    data = []
    for d in dates:
        for acc in range(100):
            data.append(
                {
                    "calculation_date": d.strftime("%Y-%m-%d"),
                    "account_id": f"CM_{acc}",
                    "account_type": "CLIENT_FUNDS",
                    "current_balance": np.random.uniform(
                        600000, 800000
                    ),  # 100 accounts * 600k = ~60M daily
                }
            )

    df = pd.DataFrame(data)

    # INJECT EXCEPTION: Shortfall on Nov 15th (Sum drops to ~40M)
    shortfall_date = "2025-11-15"
    df.loc[df["calculation_date"] == shortfall_date, "current_balance"] = 400000.00
    print(" -> CASS Seed 1 (Aggregation Shortfall): Nov 15th drops below 50M threshold")

    df.to_excel(project_dir / "client_money_ledger.xlsx", index=False)


def create_markdowns(base_dir):
    # Just a helper to copy the markdown text from the prompt into the folders
    # Assuming you paste the markdown strings from above into this function
    pass  # Implementation omitted for brevity; you can paste the text manually or script it


if __name__ == "__main__":
    base = setup_base_dir()
    generate_itgc(base)
    generate_ops_t2(base)
    generate_sox_ap(base)
    generate_wash_trade(base)
    generate_cass(base)
    print("\nAll 5 Boundary-Testing Datasets Successfully Generated!")
