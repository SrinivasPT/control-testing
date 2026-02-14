"""
Test Orchestrator with Mock AI
Validates end-to-end workflow without requiring API keys
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.orchestrator import BatchOrchestrator


def test_orchestrator_with_mock():
    """
    Tests orchestrator with mock AI translator.
    This validates the full workflow without API calls.
    """
    print("=" * 70)
    print("TEST: Orchestrator with Mock AI (No API Key Required)")
    print("=" * 70)

    # Initialize with mock AI
    orchestrator = BatchOrchestrator(
        use_mock_ai=True,  # Skip real LLM calls
        db_path="data/test_audit.db",  # Separate test database
        parquet_dir="data/test_parquet",
    )

    try:
        # Process all projects
        summary = orchestrator.process_all_projects("data/input")

        # Validate results
        assert summary["total_projects"] > 0, "No projects processed"

        print("\n" + "=" * 70)
        print("✅ TEST PASSED")
        print("=" * 70)
        print("\nValidation:")
        print(f"  - Projects processed: {summary['total_projects']}")
        print(f"  - Pass: {summary['pass_count']}")
        print(f"  - Fail: {summary['fail_count']}")
        print(f"  - Error: {summary['error_count']}")
        print(f"  - Skipped: {summary['skipped_count']}")

        # Check audit database was populated
        from src.storage.audit_fabric import AuditFabric

        audit = AuditFabric(db_path="data/test_audit.db")
        stats = audit.get_dashboard_stats()
        print("\nAudit Database Stats:")
        print(f"  - Total controls in DB: {stats['total_controls']}")
        audit.close()

        return True

    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ TEST FAILED")
        print("=" * 70)
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        orchestrator.close()


def test_orchestrator_with_missing_project():
    """Tests error handling when input directory doesn't exist"""
    print("\n" + "=" * 70)
    print("TEST: Error Handling - Missing Input Directory")
    print("=" * 70)

    orchestrator = BatchOrchestrator(use_mock_ai=True)

    try:
        orchestrator.process_all_projects("data/nonexistent")
        print("❌ Should have raised FileNotFoundError")
        return False
    except FileNotFoundError as e:
        print(f"✅ Correctly caught error: {e}")
        return True
    finally:
        orchestrator.close()


if __name__ == "__main__":
    success = True

    # Test 1: Normal workflow with mock AI
    if not test_orchestrator_with_mock():
        success = False

    # Test 2: Error handling
    if not test_orchestrator_with_missing_project():
        success = False

    # Exit with appropriate code
    if success:
        print("\n" + "=" * 70)
        print("🎉 ALL TESTS PASSED")
        print("=" * 70)
        sys.exit(0)
    else:
        print("\n" + "=" * 70)
        print("💥 SOME TESTS FAILED")
        print("=" * 70)
        sys.exit(1)
