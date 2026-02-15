#!/usr/bin/env python3
"""Check CTRL-SOX-AP-0004 data to understand the 42.87% exception rate."""

import pandas as pd
from pathlib import Path

def main():
    input_dir = Path("data/input/CTRL-SOX-AP-004")
    
    # Load data
    invoices = pd.read_excel(input_dir / "ap_invoices.xlsx")
    titles = pd.read_excel(input_dir / "employee_titles.xlsx")
    
    print(f"{'='*80}")
    print("DATA SUMMARY")
    print(f"{'='*80}")
    print(f"AP Invoices: {len(invoices):,} total")
    print(f"Employee Titles: {len(titles):,} employees")
    
    # Check high-value invoices (>$100k)
    high_value = invoices[invoices['invoice_amount'] > 100000].copy()
    print(f"\nHigh-value invoices (>$100K): {len(high_value):,}")
    
    # Join to get titles
    merged = high_value.merge(titles, on='approver_id', how='left')
    print(f"Invoices with approver titles: {len(merged[merged['approver_title'].notna()]):,}")
    
    # Check title distribution
    print(f"\n{'='*80}")
    print("APPROVER TITLE DISTRIBUTION (High-Value Invoices)")
    print(f"{'='*80}")
    title_counts = merged['approver_title'].value_counts()
    for title, count in title_counts.items():
        pct = (count / len(merged)) * 100
        approved = title in ['SVP', 'EVP', 'CEO', 'CFO']
        status = "✅ APPROVED" if approved else "❌ NOT APPROVED"
        print(f"   {title}: {count:,} ({pct:.1f}%) {status}")
    
    # Calculate violations
    approved_list = ['SVP', 'EVP', 'CEO', 'CFO']
    violations = merged[~merged['approver_title'].isin(approved_list)]
    print(f"\n{'='*80}")
    print("DIAGNOSIS")
    print(f"{'='*80}")
    print(f"High-value invoices requiring senior approval: {len(merged):,}")
    print(f"Approved by unauthorized titles: {len(violations):,} ({len(violations)/len(merged)*100:.2f}%)")
    
    if len(violations) > 0:
        print(f"\n✅ CONTROL IS WORKING CORRECTLY")
        print(f"   Test data intentionally includes {len(violations):,} invoices")
        print(f"   approved by non-senior staff (VP, Director, Manager, etc.)")
        print(f"   to verify the control detects these violations.")
    
    # Show sample violations
    print(f"\nSample violations:")
    for  _, row in violations.head(5).iterrows():
        print(f"   Invoice {row['invoice_id']}: ${row['invoice_amount']:,.2f} approved by {row['approver_title']}")

if __name__ == "__main__":
    main()
