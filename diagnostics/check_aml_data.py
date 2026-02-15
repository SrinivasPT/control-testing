#!/usr/bin/env python3
"""Check CTRL-AML-404 data to understand the 100% exception rate."""

import pandas as pd
from pathlib import Path

def main():
    input_dir = Path("data/input/CTRL-AML-404")
    
    # Load data
    onb = pd.read_excel(input_dir / "onboarding_log.xlsx")
    edd = pd.read_excel(input_dir / "edd_tracker.xlsx")
    ofac = pd.read_excel(input_dir / "ofac_watch_list.xlsx")
    
    print(f"{'='*80}")
    print("DATA SUMMARY")
    print(f"{'='*80}")
    print(f"Onboarding Log: {len(onb):,} total customers")
    print(f"  - HIGH risk: {len(onb[onb['risk_rating'] == 'HIGH']):,}")
    print(f"  - MEDIUM risk: {len(onb[onb['risk_rating'] == 'MEDIUM']):,}")
    print(f"  - LOW risk: {len(onb[onb['risk_rating'] == 'LOW']):,}")
    
    print(f"\nEDD Tracker: {len(edd):,} records")
    print(f"OFAC Watch List: {len(ofac):,} restricted entities")
    
    # Check HIGH risk population
    high_risk = onb[onb['risk_rating'] == 'HIGH'].copy()
    print(f"\n{'='*80}")
    print("HIGH RISK CUSTOMER ANALYSIS")
    print(f"{'='*80}")
    print(f"Total HIGH risk customers: {len(high_risk):,}")
    
    # Check OFAC matches (join on tax_id)
    ofac_matches = high_risk.merge(ofac, on='tax_id', how='inner')
    print(f"\n1. OFAC Sanctions Check:")
    print(f"   HIGH risk customers on OFAC list: {len(ofac_matches)}")
    if len(ofac_matches) > 0:
        print(f"   Sample: {ofac_matches[['customer_id', 'tax_id']].head(3).to_dict('records')}")
    
    # Check EDD matches (DSL joins tax_id to customer_id - THIS IS THE BUG!)
    print(f"\n2. EDD Tracker Join Analysis:")
    print(f"   Note: DSL joins onboarding.tax_id = edd.customer_id (SEMANTIC ERROR!)")
    
    # What the DSL is doing (wrong)
    edd_wrong_join = high_risk.merge(edd, left_on='tax_id', right_on='customer_id', how='left', suffixes=('_onb', '_edd'))
    # After merge with different keys, customer_id becomes customer_id_edd
    edd_matched_wrong = edd_wrong_join[edd_wrong_join['customer_id_edd'].notna()] if 'customer_id_edd' in edd_wrong_join.columns else edd_wrong_join[edd_wrong_join['edd_completion_date'].notna()]
    print(f"   DSL's incorrect join (tax_id=customer_id): {len(edd_matched_wrong)} matches")
    
    # What it SHOULD be doing (correct)
    edd_correct_join = high_risk.merge(edd, on='customer_id', how='left', suffixes=('_onb', '_edd'))
    edd_matched_correct = edd_correct_join[edd_correct_join['edd_completion_date'].notna()]
    print(f"   Correct join (customer_id=customer_id): {len(edd_matched_correct)} matches")
    
    # Check timing for matched records
    if len(edd_matched_correct) > 0:
        edd_matched_correct['days_to_edd'] = (pd.to_datetime(edd_matched_correct['edd_completion_date']) - 
                                              pd.to_datetime(edd_matched_correct['onboarding_date'])).dt.days
        late_edd = edd_matched_correct[edd_matched_correct['days_to_edd'] > 14]
        print(f"\n3. EDD Timing Check (14-day SLA):")
        print(f"   HIGH risk with EDD records: {len(edd_matched_correct)}")
        print(f"   EDDs completed > 14 days: {len(late_edd)}")
        sample_cols = edd_matched_correct[['customer_id', 'days_to_edd']].head(3).to_dict('records')
        print(f"   Sample timing: {sample_cols}")
    
    # Final diagnosis
    print(f"\n{'='*80}")
    print("DIAGNOSIS")
    print(f"{'='*80}")
    if len(edd_matched_wrong) == 0:
        print("❌ ROOT CAUSE: DSL joins tax_id to customer_id, which are different ID schemes!")
        print("   tax_id format: TAX_0000004")
        print("   customer_id format: CUST_000004")
        print("   These will NEVER match, causing 100% EDD assertion failures.")
        print("\n✅ FIX: Change DSL join to use customer_id = customer_id")
    else:
        print("Data has other issues - check OFAC and timing violations")

if __name__ == "__main__":
    main()
