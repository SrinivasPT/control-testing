import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_test_data(num_trades=10000, num_employees=500):
    print("Generating Enterprise Test Data...")

    # ---------------------------------------------------------
    # 1. Generate HR Roster (The Dimension Table)
    # ---------------------------------------------------------
    employee_ids = [f"EMP_{str(i).zfill(4)}" for i in range(1, num_employees + 1)]

    hr_data = {
        "employee_id": employee_ids,
        "department": np.random.choice(
            ["Trading", "Compliance", "Operations"], num_employees
        ),
        "employment_status": np.random.choice(
            ["ACTIVE", "ACTIVE", "ACTIVE", "TERMINATED", "ON_LEAVE"], num_employees
        ),
    }
    df_hr = pd.DataFrame(hr_data)

    # ---------------------------------------------------------
    # 2. Generate Trade Log (The Fact Table)
    # ---------------------------------------------------------
    start_date = datetime(2025, 1, 1)
    dates = [
        start_date + timedelta(days=np.random.randint(0, 90)) for _ in range(num_trades)
    ]

    trade_data = {
        "trade_id": [f"TRD_{str(i).zfill(6)}" for i in range(1, num_trades + 1)],
        "trade_date": dates,
        "trader_id": np.random.choice(
            employee_ids[:100], num_trades
        ),  # First 100 employees are traders
        "notional_amount": np.round(np.random.uniform(1000, 100000, num_trades), 2),
        "approval_status": np.random.choice(
            ["APPROVED", "PENDING", "REJECTED"], num_trades, p=[0.8, 0.15, 0.05]
        ),
        "approver_id": np.random.choice(
            employee_ids[100:], num_trades
        ),  # Rest are managers
    }
    df_trades = pd.DataFrame(trade_data)

    # ---------------------------------------------------------
    # 3. Inject Seeded Exceptions (The Audit Trap)
    # ---------------------------------------------------------
    print("\nInjecting Known Exceptions...")

    # Scenario 1 Exception: Trade > $50k but status is PENDING
    df_trades.loc[500, "notional_amount"] = 75000.00
    df_trades.loc[500, "approval_status"] = "PENDING"
    print(
        f" -> Scenario 1 Exception seeded at Trade ID: {df_trades.loc[500, 'trade_id']}"
    )

    # Scenario 2 Exception: Trade Approved by a TERMINATED employee
    terminated_emp = df_hr[df_hr["employment_status"] == "TERMINATED"][
        "employee_id"
    ].iloc[0]
    df_trades.loc[1500, "approval_status"] = "APPROVED"
    df_trades.loc[1500, "approver_id"] = terminated_emp
    print(
        f" -> Scenario 2 Exception seeded at Trade ID: {df_trades.loc[1500, 'trade_id']} (Approver: {terminated_emp})"
    )

    # Scenario 3 Exception: Trader exceeds $2M daily limit
    rogue_trader = "EMP_0099"
    rogue_date = datetime(2025, 2, 15)

    # Overwrite a few rows to force this specific trader over the limit on this specific day
    df_trades.loc[2000:2003, "trader_id"] = rogue_trader
    df_trades.loc[2000:2003, "trade_date"] = rogue_date
    df_trades.loc[2000:2003, "notional_amount"] = 600000.00  # 4 trades * 600k = $2.4M
    print(
        f" -> Scenario 3 Exception seeded for Trader: {rogue_trader} on {rogue_date.strftime('%Y-%m-%d')} (Total: $2.4M)"
    )

    # ---------------------------------------------------------
    # 4. Save to Excel
    # ---------------------------------------------------------
    print("\nSaving files to disk...")
    df_trades.to_excel("sample_trade_log.xlsx", index=False)
    df_hr.to_excel("sample_hr_roster.xlsx", index=False)
    print("Success! Created 'sample_trade_log.xlsx' and 'sample_hr_roster.xlsx'")


if __name__ == "__main__":
    generate_test_data()
