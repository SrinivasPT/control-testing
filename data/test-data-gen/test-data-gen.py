from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


def setup_dirs():
    base = Path("./data/input")
    aml = base / "project_aml_kyc"
    mnpi = base / "project_insider_trading"
    aml.mkdir(parents=True, exist_ok=True)
    mnpi.mkdir(parents=True, exist_ok=True)
    return aml, mnpi


def generate_aml_data(project_dir, num_records=10000):
    print("Generating AML/KYC Data (3-Way Join)...")

    # 1. Onboarding Log
    tax_ids = [f"TAX_{str(i).zfill(7)}" for i in range(1, num_records + 1)]
    onboard_dates = [
        datetime(2025, 8, 1) + timedelta(days=np.random.randint(0, 30))
        for _ in range(num_records)
    ]

    onboarding = {
        "customer_id": [f"CUST_{str(i).zfill(6)}" for i in range(1, num_records + 1)],
        "tax_id": tax_ids,
        "onboarding_date": [d.strftime("%Y-%m-%d") for d in onboard_dates],
        "risk_rating": np.random.choice(
            ["LOW", "MEDIUM", "HIGH"], num_records, p=[0.7, 0.2, 0.1]
        ),
        "account_status": ["OPEN"] * num_records,
    }
    df_onboard = pd.DataFrame(onboarding)

    # 2. EDD Tracker (Only for High Risk)
    high_risk_df = df_onboard[df_onboard["risk_rating"] == "HIGH"].copy()

    edd_data = {
        "customer_id": high_risk_df["customer_id"].tolist(),
        "edd_completion_date": [
            (
                datetime.strptime(d, "%Y-%m-%d")
                + timedelta(days=np.random.randint(1, 12))
            ).strftime("%Y-%m-%d")
            for d in high_risk_df["onboarding_date"]
        ],
        "analyst_id": ["ANALYST_01"] * len(high_risk_df),
    }
    df_edd = pd.DataFrame(edd_data)

    # 3. OFAC Watch List
    ofac_data = {
        "tax_id": [
            f"TAX_{str(i).zfill(7)}"
            for i in range(num_records + 1000, num_records + 1100)
        ],
        "sanction_entity_name": ["BAD_ACTOR_LLC"] * 100,
        "listed_date": ["2020-01-01"] * 100,
    }
    df_ofac = pd.DataFrame(ofac_data)

    # --- INJECT AML EXCEPTIONS ---
    # Exception 1: High Risk, EDD SLA Breach (Took 25 days instead of 14)
    breach_idx = high_risk_df.index[0]
    breach_cust = df_onboard.loc[breach_idx, "customer_id"]
    onboard_dt = datetime.strptime(
        df_onboard.loc[breach_idx, "onboarding_date"], "%Y-%m-%d"
    )
    df_edd.loc[df_edd["customer_id"] == breach_cust, "edd_completion_date"] = (
        onboard_dt + timedelta(days=25)
    ).strftime("%Y-%m-%d")
    print(f" -> AML Seed 1 (EDD SLA Breach): {breach_cust}")

    # Exception 2: High Risk, NO EDD AT ALL
    missing_idx = high_risk_df.index[1]
    missing_cust = df_onboard.loc[missing_idx, "customer_id"]
    df_edd = df_edd[df_edd["customer_id"] != missing_cust]  # Delete the record
    print(f" -> AML Seed 2 (Missing EDD): {missing_cust}")

    # Exception 3: Onboarded Customer is on OFAC List
    ofac_idx = df_onboard.index[500]
    ofac_tax_id = df_onboard.loc[ofac_idx, "tax_id"]
    df_ofac.loc[0, "tax_id"] = ofac_tax_id  # Sneak them onto the watchlist
    print(f" -> AML Seed 3 (OFAC Violation): {df_onboard.loc[ofac_idx, 'customer_id']}")

    # Save
    df_onboard.to_excel(project_dir / "onboarding_log.xlsx", index=False)
    df_edd.to_excel(project_dir / "edd_tracker.xlsx", index=False)
    df_ofac.to_excel(project_dir / "ofac_watch_list.xlsx", index=False)


def generate_mnpi_data(project_dir, num_trades=15000):
    print("\nGenerating Insider Trading Data (Composite Joins)...")

    emp_ids = [f"EMP_{str(i).zfill(4)}" for i in range(1, 500)]
    tickers = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN", "META", "NVDA"]

    # 1. Wall Cross Register
    cross_data = {
        "employee_id": np.random.choice(emp_ids, 100),
        "ticker_symbol": np.random.choice(tickers, 100),
        "cross_date": ["2025-09-01"] * 100,
        "restriction_status": np.random.choice(
            ["RESTRICTED", "CLEARED"], 100, p=[0.4, 0.6]
        ),
        "clearance_date": ["2025-09-15"] * 100,  # Cleared two weeks later
    }
    df_cross = pd.DataFrame(cross_data).drop_duplicates(
        subset=["employee_id", "ticker_symbol"]
    )

    # 2. Personal Trade Blotter
    dates = [
        datetime(2025, 9, 1) + timedelta(days=np.random.randint(0, 30))
        for _ in range(num_trades)
    ]
    blotter = {
        "trade_id": [f"TXN_{str(i).zfill(6)}" for i in range(1, num_trades + 1)],
        "employee_id": np.random.choice(emp_ids, num_trades),
        "ticker_symbol": np.random.choice(tickers, num_trades),
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "action": np.random.choice(["BUY", "SELL"], num_trades),
    }
    df_trades = pd.DataFrame(blotter)

    # --- INJECT MNPI EXCEPTIONS ---
    # Pick an employee who was cleared on Sept 15
    cleared_emp_row = df_cross[df_cross["restriction_status"] == "CLEARED"].iloc[0]
    c_emp = cleared_emp_row["employee_id"]
    c_tick = cleared_emp_row["ticker_symbol"]

    # Exception 4: Traded BEFORE clearance (Sept 10th)
    df_trades.loc[100, "employee_id"] = c_emp
    df_trades.loc[100, "ticker_symbol"] = c_tick
    df_trades.loc[100, "trade_date"] = "2025-09-10"
    print(
        f" -> MNPI Seed 1 (Traded before clearance): Trade TXN_000101 by {c_emp} on {c_tick}"
    )

    # Exception 5: Traded while still RESTRICTED
    restricted_emp_row = df_cross[df_cross["restriction_status"] == "RESTRICTED"].iloc[
        0
    ]
    r_emp = restricted_emp_row["employee_id"]
    r_tick = restricted_emp_row["ticker_symbol"]

    df_trades.loc[200, "employee_id"] = r_emp
    df_trades.loc[200, "ticker_symbol"] = r_tick
    df_trades.loc[200, "trade_date"] = "2025-09-20"
    print(
        f" -> MNPI Seed 2 (Traded while restricted): Trade TXN_000201 by {r_emp} on {r_tick}"
    )

    # Save
    df_cross.to_excel(project_dir / "wall_cross_register.xlsx", index=False)
    df_trades.to_excel(project_dir / "personal_trade_blotter.xlsx", index=False)


if __name__ == "__main__":
    aml_dir, mnpi_dir = setup_dirs()
    # Assume Markdown files are manually copied from the prompt above into these dirs
    generate_aml_data(aml_dir)
    generate_mnpi_data(mnpi_dir)
    print("\nTest datasets completely generated.")
