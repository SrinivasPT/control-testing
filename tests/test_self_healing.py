"""
Test for AI Self-Healing Execution-Guided Self-Correction

This test demonstrates the self-healing loop:
1. Create intentionally broken DSL (references non-existent column)
2. Compile to SQL
3. Validate with DuckDB EXPLAIN (should fail)
4. Verify the validation catches the error deterministically
"""

from pathlib import Path

from src.compiler.sql_compiler import ControlCompiler
from src.execution.engine import ExecutionEngine
from src.models.dsl import (
    ControlGovernance,
    EnterpriseControlDSL,
    EvidenceRequirements,
    PopulationPipeline,
    SemanticMapping,
    ValueMatchAssertion,
)


def test_validate_sql_dry_run_catches_invalid_column():
    """
    Test that validate_sql_dry_run catches references to non-existent columns.
    This is the DETERMINISTIC JUDGE that the AI must obey.
    """
    # Create a broken DSL that references a column that doesn't exist
    broken_dsl = EnterpriseControlDSL(
        governance=ControlGovernance(
            control_id="TEST-BROKEN-001",
            version="1.0.0",
            owner_role="Test",
            testing_frequency="Daily",
            regulatory_citations=["TEST"],
            risk_objective="Test broken DSL detection",
        ),
        ontology_bindings=[
            SemanticMapping(
                business_term="Status",
                dataset_alias="test_data",
                technical_field="status",
                data_type="string",
            ),
            SemanticMapping(
                business_term="NonExistentColumn",  # This column doesn't exist!
                dataset_alias="test_data",
                technical_field="non_existent_column",
                data_type="string",
            ),
        ],
        population=PopulationPipeline(
            base_dataset="test_data",
            steps=[],
            sampling=None,
        ),
        assertions=[
            ValueMatchAssertion(
                assertion_id="assert_001",
                assertion_type="value_match",
                description="Check non-existent column",
                field="non_existent_column",  # Broken!
                operator="eq",
                expected_value="APPROVED",
                materiality_threshold_percent=0.0,
            )
        ],
        evidence=EvidenceRequirements(
            retention_years=7,
            reviewer_workflow="Requires_Human_Signoff",
            exception_routing_queue="TEST",
        ),
    )

    # Create a test Parquet file with known columns
    import pandas as pd

    test_dir = Path("data/test_parquet")
    test_dir.mkdir(parents=True, exist_ok=True)

    # Create test data with only 'status' column (no 'non_existent_column')
    test_df = pd.DataFrame({"status": ["APPROVED", "APPROVED", "REJECTED"]})
    test_parquet = test_dir / "test_broken_validation.parquet"
    test_df.to_parquet(test_parquet, index=False)

    # Create manifest
    manifests = {
        "test_data": {
            "parquet_path": str(test_parquet),
            "sha256_hash": "test_hash_123",
            "columns": ["status"],  # Only 'status' exists
            "row_count": 3,
        }
    }

    # Compile the broken DSL to SQL
    compiler = ControlCompiler(broken_dsl)
    sql = compiler.compile_to_sql(manifests)
    print(f"\nGenerated SQL:\n{sql}\n")

    # Execute the deterministic validation
    engine = ExecutionEngine(":memory:")
    is_valid, error_msg = engine.validate_sql_dry_run(sql)

    print(f"Validation result: is_valid={is_valid}")
    print(f"Error message: {error_msg}")

    # ASSERTION: The deterministic judge should reject this SQL
    assert is_valid is False, "Validation should catch the non-existent column"
    assert (
        "non_existent_column" in error_msg.lower() or "column" in error_msg.lower()
    ), f"Error message should mention the missing column, got: {error_msg}"

    print("\n✅ DETERMINISTIC VALIDATION PASSED:")
    print(f"   DuckDB correctly rejected SQL with error: {error_msg[:100]}...")

    # Cleanup
    engine.close()
    test_parquet.unlink(missing_ok=True)


def test_validate_sql_dry_run_accepts_valid_sql():
    """
    Test that validate_sql_dry_run PASSES for correct DSL.
    This ensures we're not getting false positives.
    """
    # Create a CORRECT DSL
    correct_dsl = EnterpriseControlDSL(
        governance=ControlGovernance(
            control_id="TEST-CORRECT-001",
            version="1.0.0",
            owner_role="Test",
            testing_frequency="Daily",
            regulatory_citations=["TEST"],
            risk_objective="Test correct DSL validation",
        ),
        ontology_bindings=[
            SemanticMapping(
                business_term="Status",
                dataset_alias="test_data",
                technical_field="status",
                data_type="string",
            ),
        ],
        population=PopulationPipeline(
            base_dataset="test_data",
            steps=[],
            sampling=None,
        ),
        assertions=[
            ValueMatchAssertion(
                assertion_id="assert_001",
                assertion_type="value_match",
                description="Check status column",
                field="status",  # This exists!
                operator="eq",
                expected_value="APPROVED",
                materiality_threshold_percent=0.0,
            )
        ],
        evidence=EvidenceRequirements(
            retention_years=7,
            reviewer_workflow="Requires_Human_Signoff",
            exception_routing_queue="TEST",
        ),
    )

    # Create test data
    from pathlib import Path

    import pandas as pd

    test_dir = Path("data/test_parquet")
    test_dir.mkdir(parents=True, exist_ok=True)

    test_df = pd.DataFrame({"status": ["APPROVED", "APPROVED", "REJECTED"]})
    test_parquet = test_dir / "test_correct_validation.parquet"
    test_df.to_parquet(test_parquet, index=False)

    manifests = {
        "test_data": {
            "parquet_path": str(test_parquet),
            "sha256_hash": "test_hash_456",
            "columns": ["status"],
            "row_count": 3,
        }
    }

    # Compile and validate
    compiler = ControlCompiler(correct_dsl)
    sql = compiler.compile_to_sql(manifests)

    engine = ExecutionEngine(":memory:")
    is_valid, error_msg = engine.validate_sql_dry_run(sql)

    print(f"\nValidation result: is_valid={is_valid}")
    print(f"Message: {error_msg}")

    # ASSERTION: Valid SQL should pass
    assert is_valid is True, f"Valid SQL should pass validation, got error: {error_msg}"
    assert error_msg == "Valid", f"Expected 'Valid', got: {error_msg}"

    print("\n✅ VALID SQL CORRECTLY ACCEPTED")

    # Cleanup
    engine.close()
    test_parquet.unlink(missing_ok=True)


if __name__ == "__main__":
    print("=" * 80)
    print("Testing Deterministic SQL Dry-Run Validation")
    print("=" * 80)

    print("\n[Test 1] Testing detection of broken SQL...")
    test_validate_sql_dry_run_catches_invalid_column()

    print("\n" + "=" * 80)
    print("\n[Test 2] Testing acceptance of valid SQL...")
    test_validate_sql_dry_run_accepts_valid_sql()

    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED - Self-Healing Infrastructure Ready!")
    print("=" * 80)
