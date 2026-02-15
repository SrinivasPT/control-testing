"""
Result Formatter Module
Single Responsibility: Format execution results and summary reports
"""

from typing import Any, Dict, List, Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class ResultFormatter:
    """
    Formats execution results and summary reports.
    Pure formatting - no business logic.
    """

    @staticmethod
    def format_project_result(
        project_name: str,
        control_id: str,
        verdict: str,
        exception_count: int = 0,
        total_population: int = 0,
        exception_rate: float = 0.0,
        dsl_cached: bool = False,
        error: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Creates standardized project result dictionary.

        Args:
            project_name: Project folder name
            control_id: Control identifier
            verdict: PASS/FAIL/ERROR/SKIPPED
            exception_count: Number of exceptions found
            total_population: Total population size
            exception_rate: Exception rate percentage
            dsl_cached: Whether DSL was retrieved from cache
            error: Error message (for ERROR verdict)
            reason: Reason for skipping (for SKIPPED verdict)

        Returns:
            Standardized result dictionary
        """
        result: Dict[str, Any] = {
            "project_name": project_name,
            "control_id": control_id,
            "verdict": verdict,
        }

        if verdict in ["PASS", "FAIL"]:
            result["exception_count"] = exception_count
            result["total_population"] = total_population
            result["exception_rate"] = exception_rate
            result["dsl_cached"] = dsl_cached
        elif verdict == "ERROR" and error is not None:
            result["error"] = error
        elif verdict == "SKIPPED" and reason is not None:
            result["reason"] = reason

        return result

    @staticmethod
    def print_project_result(result: Dict[str, Any]):
        """
        Prints formatted project result to console.

        Args:
            result: Project result dictionary
        """
        verdict_emoji = {
            "PASS": "✅",
            "FAIL": "❌",
            "ERROR": "⚠️",
            "SKIPPED": "⏭️",
        }
        emoji = verdict_emoji.get(result["verdict"], "❓")
        print(f"\n{emoji} VERDICT: {result['verdict']}")

        if result["verdict"] in ["PASS", "FAIL"]:
            print(
                f"   Exceptions: {result['exception_count']}/{result['total_population']} "
                f"({result['exception_rate']:.2f}%)"
            )
        elif result["verdict"] == "ERROR":
            print(f"   Error: {result.get('error', 'Unknown')}")
        elif result["verdict"] == "SKIPPED":
            print(f"   Reason: {result.get('reason', 'Unknown')}")

    @staticmethod
    def generate_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregates execution results into summary statistics.

        Args:
            results: List of project result dictionaries

        Returns:
            Summary statistics dictionary
        """
        total = len(results)
        pass_count = sum(1 for r in results if r["verdict"] == "PASS")
        fail_count = sum(1 for r in results if r["verdict"] == "FAIL")
        error_count = sum(1 for r in results if r["verdict"] == "ERROR")
        skipped_count = sum(1 for r in results if r["verdict"] == "SKIPPED")

        cached_dsl_count = sum(1 for r in results if r.get("dsl_cached", False))
        generated_dsl_count = total - cached_dsl_count - error_count - skipped_count

        return {
            "total_projects": total,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "error_count": error_count,
            "skipped_count": skipped_count,
            "cached_dsl_count": cached_dsl_count,
            "generated_dsl_count": generated_dsl_count,
            "results": results,
        }

    @staticmethod
    def print_summary(summary: Dict[str, Any]):
        """
        Prints formatted summary report to console.

        Args:
            summary: Summary statistics dictionary
        """
        print(f"\n{'=' * 60}")
        print("📊 EXECUTION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total Projects:      {summary['total_projects']}")
        print(f"  ✅ PASS:           {summary['pass_count']}")
        print(f"  ❌ FAIL:           {summary['fail_count']}")
        print(f"  ⚠️  ERROR:          {summary['error_count']}")
        print(f"  ⏭️  SKIPPED:        {summary['skipped_count']}")
        print("\nDSL Generation:")
        print(f"  🔄 Cached (DB):    {summary['cached_dsl_count']}")
        print(f"  🤖 AI Generated:   {summary['generated_dsl_count']}")
        print(f"{'=' * 60}\n")
