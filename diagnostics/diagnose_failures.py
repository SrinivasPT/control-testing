#!/usr/bin/env python3
"""Diagnostic script to analyze control execution results."""

import sqlite3
import json
from pathlib import Path

def main():
    db_path = Path("data/audit.db")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Get problematic controls
    cursor = conn.execute("""
        SELECT control_id, verdict, exception_count, total_population, 
               exceptions_sample, error_message
        FROM executions 
        WHERE control_id IN ('CTRL-AML-404', 'CTRL-908101', 'CTRL-SOX-AP-004')
        ORDER BY control_id
    """)
    
    for row in cursor:
        rate = (row['exception_count'] / row['total_population'] * 100) if row['total_population'] > 0 else 0
        print(f"\n{'='*80}")
        print(f"Control: {row['control_id']}")
        print(f"Verdict: {row['verdict']}")
        print(f"Population: {row['total_population']:,}")
        print(f"Exceptions: {row['exception_count']:,} ({rate:.2f}%)")
        print(f"\nSample Exceptions (first 3 rows):")
        print("-" * 80)
        
        if row['exceptions_sample']:
            # Parse CSV
            lines = row['exceptions_sample'].split('\n')[:5]  # Header + 3 rows
            for line in lines:
                print(line[:150])  # Truncate long lines
        else:
            print("No exception sample available")
    
    conn.close()

if __name__ == "__main__":
    main()
