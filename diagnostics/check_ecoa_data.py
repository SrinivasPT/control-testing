#!/usr/bin/env python3
"""Check CTRL-908101 data to understand the 99% exception rate."""

import pandas as pd
from pathlib import Path

def main():
    input_dir = Path("data/input/CTRL-ECOA-908101")
    
    # Load data
    df = pd.read_excel(input_dir / "campaign_reconciliation_report.xlsx")
    
    print(f"{'='*80}")
    print("DATA SUMMARY")
    print(f"{'='*80}")
    print(f"Total Records: {len(df):,}")
    
    # Check reconciliation status distribution
    print(f"\nReconciliation Status Distribution:")
    status_counts = df['reconciliation_status'].value_counts()
    for status, count in status_counts.items():
        pct = (count / len(df)) * 100
        print(f"   {status}: {count:,} ({pct:.1f}%)")
    
    # Check letter type distribution for mismatches
    mismatches = df[df['reconciliation_status'] == 'MISMATCH']
    print(f"\n{'='*80}")
    print("MISMATCH ANALYSIS")
    print(f"{'='*80}")
    print(f"Total MISMATCHes: {len(mismatches):,}")
    
    if len(mismatches) > 0:
        letter_types = mismatches['letter_type'].value_counts()
        print(f"\nLetter Type Distribution (MISMATCHes):")
        for ltype, count in letter_types.items():
            pct = (count / len(mismatches)) * 100
            print(f"   {ltype}: {count:,} ({pct:.1f}%)")
        
        # Check BREF raised
        print(f"\nBREF Raised for MISMATCHes:")
        bref_counts = mismatches['bref_raised_flag'].value_counts()
        for flag, count in bref_counts.items():
            pct = (count / len(mismatches)) * 100
            print(f"   {flag}: {count:,} ({pct:.1f}%)")
        
        # Check timing for resolved mismatches
        resolved = mismatches[mismatches['resolution_date'].notna()].copy()
        if len(resolved) > 0:
            resolved['application_date'] = pd.to_datetime(resolved['application_date'])
            resolved['resolution_date'] = pd.to_datetime(resolved['resolution_date'])
            resolved['days_to_resolve'] = (resolved['resolution_date'] - resolved['application_date']).dt.days
            
            late_resolutions = resolved[resolved['days_to_resolve'] > 30]
            print(f"\nResolution Timing:")
            print(f"   Resolved MISMATCHes: {len(resolved):,}")
            print(f"   Resolved > 30 days: {len(late_resolutions):,}")
            
            if len(resolved) > 0:
                print(f"   Sample timing: {resolved[['record_id', 'days_to_resolve']].head(3).to_dict('records')}")
        
        print(f"\n{'='*80}")
        print("DIAGNOSIS")
        print(f"{'='*80}")
        
        # Calculate violations
        adverse_action_mismatches = mismatches[mismatches['letter_type'] == 'Adverse Action']
        print(f"Adverse Action MISMATCHes: {len(adverse_action_mismatches):,}")
        
        if len(adverse_action_mismatches) > 0:
            no_bref = adverse_action_mismatches[adverse_action_mismatches['bref_raised_flag'] != 'Y']
            print(f"   Missing BREF (bref_raised_flag != 'Y'): {len(no_bref):,}")
            
            has_bref = adverse_action_mismatches[adverse_action_mismatches['bref_raised_flag'] == 'Y']
            resolved_bref = has_bref[has_bref['resolution_date'].notna()]
            if len(resolved_bref) > 0:
                resolved_bref_copy = resolved_bref.copy()
                resolved_bref_copy['application_date'] = pd.to_datetime(resolved_bref_copy['application_date'])
                resolved_bref_copy['resolution_date'] = pd.to_datetime(resolved_bref_copy['resolution_date'])
                resolved_bref_copy['days_to_resolve'] = (resolved_bref_copy['resolution_date'] - resolved_bref_copy['application_date']).dt.days
                late_fixed = resolved_bref_copy[resolved_bref_copy['days_to_resolve'] > 30]
                print(f"   BREF raised but resolved > 30 days: {len(late_fixed):,}")
            
            print(f"\n✅ CONTROL IS WORKING - detecting ECOA violations")
            print(f"   Test data includes MISMATCHes without BREF or late resolutions")

if __name__ == "__main__":
    main()
