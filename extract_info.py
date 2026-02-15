#!/usr/bin/env python3
"""
Comprehensive Control Review Report Generator

Extracts and analyzes:
- Control metadata and procedures
- Excel data structures
- Generated DSL with pipeline analysis
- Compiled SQL with complexity metrics
- Execution results and exception rates
- Quality checks and issue detection
- Overall system health dashboard
"""

import json
import re
import sqlite3
from pathlib import Path

import pandas as pd


def get_excel_structure_and_sample(excel_path: Path, rows: int = 3) -> dict:
    """Get Excel file structure with statistics."""
    try:
        df = pd.read_excel(excel_path)
        return {
            "columns": list(df.columns),
            "row_count": len(df),
            "sample": df.head(rows).to_string(index=False),
            "error": None,
        }
    except Exception as e:
        return {"columns": [], "row_count": 0, "sample": "", "error": str(e)}


def analyze_dsl(dsl: dict, control_id: str) -> dict:
    """Extract key metrics from DSL for analysis."""
    analysis = {
        "control_id": control_id,
        "population_steps": 0,
        "assertion_count": 0,
        "assertion_types": [],
        "join_count": 0,
        "uses_sampling": False,
        "data_sources": set(),
    }

    # Get base dataset
    population = dsl.get("population", {})
    base_dataset = population.get("base_dataset", "")
    if base_dataset:
        analysis["data_sources"].add(base_dataset)

    # Analyze population pipeline steps
    steps = population.get("steps", [])
    for step in steps:
        action = step.get("action", {})
        op = action.get("operation", "")

        if "join" in op:
            analysis["join_count"] += 1
            analysis["population_steps"] += 1
            # Add right dataset to sources
            right_ds = action.get("right_dataset", "")
            if right_ds:
                analysis["data_sources"].add(right_ds)
        elif "filter" in op:
            analysis["population_steps"] += 1
        elif "sample" in op:
            analysis["uses_sampling"] = True
            analysis["population_steps"] += 1

    # Analyze assertions
    assertions = dsl.get("assertions", [])
    analysis["assertion_count"] = len(assertions)
    for assertion in assertions:
        # Get assertion_type directly from assertion object
        assertion_type = assertion.get("assertion_type", "unknown")
        analysis["assertion_types"].append(assertion_type)

    analysis["data_sources"] = list(analysis["data_sources"])
    return analysis


def analyze_sql(sql: str) -> dict:
    """Extract complexity metrics from compiled SQL."""
    return {
        "cte_count": len(
            re.findall(r"\bWITH\b|\),\s*\w+\s+AS\s+\(", sql, re.IGNORECASE)
        ),
        "join_count": len(
            re.findall(r"\bLEFT JOIN\b|\bINNER JOIN\b", sql, re.IGNORECASE)
        ),
        "where_clauses": len(re.findall(r"\bWHERE\b", sql, re.IGNORECASE)),
        "is_not_true_count": len(re.findall(r"\bIS NOT TRUE\b", sql, re.IGNORECASE)),
        "exclude_clause_count": len(re.findall(r"\bEXCLUDE\s+\(", sql, re.IGNORECASE)),
        "lines": len(sql.split("\n")),
    }


def check_execution_quality(exec_data: dict, dsl_analysis: dict) -> list[str]:
    """Identify potential issues with execution results."""
    issues = []

    # Check for errors
    if exec_data["verdict"] == "ERROR":
        issues.append(
            f"🔴 EXECUTION ERROR: {exec_data.get('error_message', 'Unknown error')}"
        )
        return issues

    # Check exception rate
    if exec_data["total_population"] > 0:
        exception_rate = (
            exec_data["exception_count"] / exec_data["total_population"]
        ) * 100

        # Suspicious patterns
        if exception_rate == 100.0:
            issues.append(
                "⚠️ 100% exception rate - Entire population failed. Check if Population filter is too restrictive or assertions are inverted."
            )
        elif exception_rate == 0.0 and dsl_analysis["assertion_count"] > 2:
            issues.append(
                f"⚠️ 0% exception rate with {dsl_analysis['assertion_count']} assertions - Control may not be testing effectively."
            )
        elif exception_rate > 90.0:
            issues.append(
                f"⚠️ Very high exception rate ({exception_rate:.1f}%) - Review if this is expected or indicates data quality issues."
            )
    else:
        issues.append(
            "🔴 ZERO POPULATION - Population filter excluded all records. Control did not test anything."
        )

    # Check assertion logic
    if dsl_analysis["assertion_count"] == 0:
        issues.append("🔴 NO ASSERTIONS - DSL has no test logic defined.")

    return issues


def format_execution_stats(exec_data: dict) -> str:
    """Format execution statistics as a markdown table."""
    if exec_data["verdict"] == "ERROR":
        return f"""
| Metric | Value |
|--------|-------|
| **Verdict** | ❌ ERROR |
| **Error** | {exec_data.get("error_message", "Unknown")} |
| **Timestamp** | {exec_data["executed_at"]} |
"""

    exception_rate = 0.0
    if exec_data["total_population"] > 0:
        exception_rate = (
            exec_data["exception_count"] / exec_data["total_population"]
        ) * 100

    verdict_emoji = "✅" if exec_data["verdict"] == "PASS" else "❌"

    return f"""
| Metric | Value |
|--------|-------|
| **Verdict** | {verdict_emoji} {exec_data["verdict"]} |
| **Total Population** | {exec_data["total_population"]:,} |
| **Exceptions Found** | {exec_data["exception_count"]:,} |
| **Exception Rate** | {exception_rate:.2f}% |
| **Timestamp** | {exec_data["executed_at"]} |
"""


def generate_dashboard(all_stats: list[dict]) -> str:
    """Generate overall system health dashboard."""
    total_controls = len(all_stats)
    error_count = sum(1 for s in all_stats if s["verdict"] == "ERROR")
    pass_count = sum(1 for s in all_stats if s["verdict"] == "PASS")
    fail_count = sum(1 for s in all_stats if s["verdict"] == "FAIL")

    total_population = sum(
        s["total_population"] for s in all_stats if s["verdict"] != "ERROR"
    )
    total_exceptions = sum(
        s["exception_count"] for s in all_stats if s["verdict"] != "ERROR"
    )

    avg_exception_rate = 0.0
    if total_population > 0:
        avg_exception_rate = (total_exceptions / total_population) * 100

    # Assertion type distribution
    all_assertion_types = []
    for s in all_stats:
        all_assertion_types.extend(s.get("assertion_types", []))

    assertion_dist = {}
    for atype in all_assertion_types:
        assertion_dist[atype] = assertion_dist.get(atype, 0) + 1

    dashboard = f"""# 📊 Enterprise Control Execution Dashboard

## System Health Overview

| Metric | Value |
|--------|-------|
| **Total Controls Processed** | {total_controls} |
| **✅ PASS** | {pass_count} ({pass_count / total_controls * 100:.1f}%) |
| **❌ FAIL** | {fail_count} ({fail_count / total_controls * 100:.1f}%) |
| **🔴 ERROR** | {error_count} ({error_count / total_controls * 100:.1f}%) |
| **Total Records Tested** | {total_population:,} |
| **Total Exceptions Detected** | {total_exceptions:,} |
| **Average Exception Rate** | {avg_exception_rate:.2f}% |

## DSL Generation Analysis

| Control ID | Population Steps | Joins | Assertions | Data Sources | Sampling |
|------------|------------------|-------|------------|--------------|----------|
"""

    for s in all_stats:
        sampling_icon = "✓" if s.get("uses_sampling", False) else "✗"
        data_src_count = len(s.get("data_sources", []))
        dashboard += f"| {s['control_id']} | {s.get('population_steps', 0)} | {s.get('join_count', 0)} | {s.get('assertion_count', 0)} | {data_src_count} | {sampling_icon} |\n"

    dashboard += "\n## SQL Complexity Metrics\n\n"
    dashboard += (
        "| Control ID | CTEs | Joins | WHERE | IS NOT TRUE | EXCLUDE | Lines |\n"
    )
    dashboard += (
        "|------------|------|-------|-------|-------------|---------|-------|\n"
    )

    for s in all_stats:
        sql_metrics = s.get("sql_metrics", {})
        dashboard += f"| {s['control_id']} | {sql_metrics.get('cte_count', 0)} | {sql_metrics.get('join_count', 0)} | {sql_metrics.get('where_clauses', 0)} | {sql_metrics.get('is_not_true_count', 0)} | {sql_metrics.get('exclude_clause_count', 0)} | {sql_metrics.get('lines', 0)} |\n"

    dashboard += "\n## Assertion Type Distribution\n\n"
    for atype, count in sorted(
        assertion_dist.items(), key=lambda x: x[1], reverse=True
    ):
        dashboard += f"- **{atype}**: {count} assertions\n"

    dashboard += "\n## Quality Checks\n\n"

    total_issues = sum(len(s.get("quality_issues", [])) for s in all_stats)
    if total_issues == 0:
        dashboard += "✅ **No quality issues detected across all controls.**\n\n"
    else:
        dashboard += f"⚠️ **{total_issues} potential issues detected:**\n\n"
        for s in all_stats:
            issues = s.get("quality_issues", [])
            if issues:
                dashboard += f"### {s['control_id']}\n"
                for issue in issues:
                    dashboard += f"- {issue}\n"
                dashboard += "\n"

    dashboard += "---\n\n"
    return dashboard


def main():
    workspace_root = Path(__file__).parent
    db_path = workspace_root / "data" / "audit.db"
    input_dir = workspace_root / "data" / "input"

    # Connect to database
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Get all controls with DSL
    controls_cursor = conn.execute("SELECT control_id, dsl_json FROM controls")
    controls = {
        row["control_id"]: json.loads(row["dsl_json"]) for row in controls_cursor
    }

    # Get executions with full details
    executions_cursor = conn.execute("""
        SELECT control_id, compiled_sql, verdict, exception_count, 
               total_population, executed_at, error_message
        FROM executions 
        ORDER BY control_id, executed_at DESC
    """)

    executions = {}
    for row in executions_cursor:
        control_id = row["control_id"]
        if control_id not in executions:
            executions[control_id] = []
        executions[control_id].append(dict(row))

    conn.close()

    # Collect stats for dashboard
    all_stats = []

    # Generate markdown
    markdown_lines = []

    for ctrl_dir in sorted(input_dir.iterdir()):
        if not ctrl_dir.is_dir() or not ctrl_dir.name.startswith("CTRL-"):
            continue

        control_id = ctrl_dir.name
        markdown_lines.append(f"## Project: {control_id}\n")

        # Get DSL analysis
        dsl_analysis = {}
        if control_id in controls:
            dsl_analysis = analyze_dsl(controls[control_id], control_id)

        # Get latest execution data
        exec_data = None
        if control_id in executions and executions[control_id]:
            exec_data = executions[control_id][0]  # Most recent

        # Combine stats
        control_stats = {
            "control_id": control_id,
            "verdict": exec_data.get("verdict", "UNKNOWN") if exec_data else "NO_EXEC",
            "total_population": exec_data.get("total_population", 0)
            if exec_data
            else 0,
            "exception_count": exec_data.get("exception_count", 0) if exec_data else 0,
            **dsl_analysis,
        }

        # SQL analysis
        if exec_data and exec_data.get("compiled_sql"):
            control_stats["sql_metrics"] = analyze_sql(exec_data["compiled_sql"])

        # Quality checks
        if exec_data and dsl_analysis:
            control_stats["quality_issues"] = check_execution_quality(
                exec_data, dsl_analysis
            )

        all_stats.append(control_stats)

        # Control Information
        control_md_path = ctrl_dir / "control-information.md"
        if control_md_path.exists():
            with open(control_md_path, "r") as f:
                control_info = f.read()
            markdown_lines.append("### 📋 Control Information\n")
            markdown_lines.append(control_info)
            markdown_lines.append("\n")

        # Execution Results (moved up for visibility)
        if exec_data:
            markdown_lines.append("### 📊 Execution Results\n")
            markdown_lines.append(format_execution_stats(exec_data))

            # Quality issues
            if control_stats.get("quality_issues"):
                markdown_lines.append("\n#### ⚠️ Quality Checks\n")
                for issue in control_stats["quality_issues"]:
                    markdown_lines.append(f"- {issue}\n")
                markdown_lines.append("\n")

        # DSL Analysis Summary
        if dsl_analysis:
            markdown_lines.append("### 🔍 DSL Analysis\n")
            markdown_lines.append(
                f"- **Population Steps:** {dsl_analysis.get('population_steps', 0)}\n"
            )
            markdown_lines.append(f"- **Joins:** {dsl_analysis.get('join_count', 0)}\n")
            markdown_lines.append(
                f"- **Assertions:** {dsl_analysis.get('assertion_count', 0)} ({', '.join(dsl_analysis.get('assertion_types', []))})\n"
            )
            markdown_lines.append(
                f"- **Data Sources:** {', '.join(dsl_analysis.get('data_sources', ['None']))}\n"
            )
            markdown_lines.append(
                f"- **Sampling Enabled:** {'Yes' if dsl_analysis.get('uses_sampling') else 'No'}\n\n"
            )

        # Excel Files
        excel_files = list(ctrl_dir.glob("*.xlsx"))
        if excel_files:
            markdown_lines.append("### 📁 Excel File Structures\n")
            for excel_file in sorted(excel_files):
                markdown_lines.append(f"#### {excel_file.name}\n")
                excel_info = get_excel_structure_and_sample(excel_file)
                if excel_info["error"]:
                    markdown_lines.append(f"❌ Error: {excel_info['error']}\n\n")
                else:
                    markdown_lines.append(
                        f"**Rows:** {excel_info['row_count']:,} | **Columns:** {len(excel_info['columns'])}\n\n"
                    )
                    markdown_lines.append(
                        f"**Column Names:** {', '.join(excel_info['columns'])}\n\n"
                    )
                    markdown_lines.append(
                        f"**Sample Data:**\n```\n{excel_info['sample']}\n```\n\n"
                    )

        # DSL (Full JSON)
        if control_id in controls:
            markdown_lines.append("### 🤖 Generated DSL (Full JSON)\n")
            dsl_json = json.dumps(controls[control_id], indent=2)
            markdown_lines.append(f"```json\n{dsl_json}\n```\n\n")

        # Compiled SQL
        if control_id in executions:
            markdown_lines.append("### 💾 Compiled SQL\n")
            for i, exec_record in enumerate(executions[control_id], 1):
                if i > 1:  # Show only most recent by default
                    markdown_lines.append(
                        f"<details><summary>Execution {i} (Historical)</summary>\n\n"
                    )
                markdown_lines.append(f"```sql\n{exec_record['compiled_sql']}\n```\n\n")
                if i > 1:
                    markdown_lines.append("</details>\n\n")

        markdown_lines.append("---\n\n")

    # Generate dashboard at the beginning
    dashboard = generate_dashboard(all_stats)
    final_markdown = dashboard + "\n".join(markdown_lines)

    # Write to file
    output_path = workspace_root / "control_review.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_markdown)

    print(f"✅ Comprehensive review document generated: {output_path}")
    print(f"📊 Analyzed {len(all_stats)} controls")
    print(
        f"⚠️  Detected {sum(len(s.get('quality_issues', [])) for s in all_stats)} potential issues"
    )


if __name__ == "__main__":
    main()
