"""
Test: Population Count with Joins

Verifies that _get_population_count() correctly handles controls with LEFT JOINs.
"""

import tempfile
from pathlib import Path

import pandas as pd

from src.compiler.sql_compiler import ControlCompiler
from src.execution.engine import ExecutionEngine
from src.execution.ingestion import EvidenceIngestion
from src.models.dsl import (
    ControlGovernance,
    EnterpriseControlDSL,
    EvidenceRequirements,
    FilterIsNull,
    JoinLeft,
    PopulationPipeline,
    PopulationPipelineStep,
    SemanticMapping,
    ValueMatchAssertion,
)


def test_population_count_with_join():
    """
    Test that population count works when control has LEFT JOIN with filters
    on joined columns (the bug scenario).
    """

    # Create test data
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Base dataset: trades
        trades_df = pd.DataFrame(
            {
                "trade_id": [1, 2, 3, 4, 5],
                "employee_id": ["E001", "E002", "E003", "E004", "E005"],
                "ticker": ["AAPL", "TSLA", "AAPL", "MSFT", "GOOGL"],
                "trade_date": [
                    "2024-01-15",
                    "2024-01-16",
                    "2024-01-17",
                    "2024-01-18",
                    "2024-01-19",
                ],
            }
        )

        # Joined dataset: restrictions (wall-cross register)
        restrictions_df = pd.DataFrame(
            {
                "employee_id": ["E001", "E003"],  # Only 2 matches
                "ticker": ["AAPL", "AAPL"],
                "restriction_status": ["ACTIVE", "ACTIVE"],
            }
        )

        # Ingest to Parquet
        ingestion = EvidenceIngestion(storage_dir=str(tmpdir))

        trades_path = tmpdir / "trades.xlsx"
        trades_df.to_excel(trades_path, index=False, sheet_name="Sheet1")

        restrictions_path = tmpdir / "restrictions.xlsx"
        restrictions_df.to_excel(restrictions_path, index=False, sheet_name="Sheet1")

        trades_manifests = ingestion.ingest_excel_to_parquet(
            str(trades_path), "trades", "TEST_SYSTEM"
        )
        restrictions_manifests = ingestion.ingest_excel_to_parquet(
            str(restrictions_path), "restrictions", "TEST_SYSTEM"
        )

        manifests = {
            "trades_sheet1": trades_manifests[0],
            "restrictions_sheet1": restrictions_manifests[0],
        }

        # Create DSL with JOIN and filter on joined column
        dsl = EnterpriseControlDSL(
            governance=ControlGovernance(
                control_id="TEST-JOIN-001",
                version="1.0",
                owner_role="Test Owner",
                testing_frequency="Daily",
                regulatory_citations=["TEST-REG-001"],
                risk_objective="Test join population counting",
            ),
            ontology_bindings=[
                SemanticMapping(
                    business_term="Trade ID",
                    dataset_alias="trades_sheet1",
                    technical_field="trade_id",
                    data_type="string",
                ),
                SemanticMapping(
                    business_term="Employee",
                    dataset_alias="trades_sheet1",
                    technical_field="employee_id",
                    data_type="string",
                ),
                SemanticMapping(
                    business_term="Restriction Status",
                    dataset_alias="restrictions_sheet1",
                    technical_field="restriction_status",
                    data_type="string",
                ),
            ],
            population=PopulationPipeline(
                base_dataset="trades_sheet1",
                steps=[
                    PopulationPipelineStep(
                        step_id="join_restrictions",
                        action=JoinLeft(
                            operation="join_left",
                            left_dataset="trades_sheet1",
                            right_dataset="restrictions_sheet1",
                            left_keys=["employee_id", "ticker"],
                            right_keys=["employee_id", "ticker"],
                        ),
                    ),
                    PopulationPipelineStep(
                        step_id="filter_matches",
                        action=FilterIsNull(
                            operation="filter_is_null",
                            field="join_restrictions_employee_id",  # Joined column!
                            is_null=False,  # Only keep trades that match restrictions
                        ),
                    ),
                ],
            ),
            assertions=[
                ValueMatchAssertion(
                    assertion_type="value_match",
                    assertion_id="check_status",
                    description="Restriction must be ACTIVE",
                    field="restriction_status",
                    operator="eq",
                    expected_value="ACTIVE",
                    materiality_threshold_percent=0.0,
                )
            ],
            evidence=EvidenceRequirements(
                retention_years=7,
                reviewer_workflow="Auto-Close_If_Pass",
                exception_routing_queue="test_queue",
            ),
        )

        # Execute
        engine = ExecutionEngine()

        try:
            # This should NOT crash with "column not found" error
            # (That was the bug)
            compiler = ControlCompiler(dsl)
            population_count = engine._get_population_count(manifests, dsl, compiler)

            # Expected: 2 (trades from E001 and E003 with AAPL)
            # because those are the only matches after LEFT JOIN + IS NOT NULL filter
            assert population_count == 2, f"Expected 2, got {population_count}"

            print(f"✅ Population count correct: {population_count}")

            # Now test full execution
            report = engine.execute_control(dsl, manifests)

            assert report["verdict"] in ["PASS", "FAIL", "ERROR"]
            assert report["total_population"] == 2
            assert report["exception_count"] >= 0

            print("✅ Full execution successful:")
            print(f"   Verdict: {report['verdict']}")
            print(f"   Population: {report['total_population']}")
            print(f"   Exceptions: {report['exception_count']}")

        finally:
            engine.close()


def test_population_count_without_join():
    """
    Test that population count still works for simple controls without joins.
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Simple dataset
        data_df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "ACTIVE"],
            }
        )

        ingestion = EvidenceIngestion(storage_dir=str(tmpdir))

        data_path = tmpdir / "data.xlsx"
        data_df.to_excel(data_path, index=False, sheet_name="Sheet1")

        manifests_list = ingestion.ingest_excel_to_parquet(
            str(data_path), "data", "TEST_SYSTEM"
        )

        manifests = {"data_sheet1": manifests_list[0]}

        # Simple DSL (no joins)
        dsl = EnterpriseControlDSL(
            governance=ControlGovernance(
                control_id="TEST-SIMPLE-001",
                version="1.0",
                owner_role="Test Owner",
                testing_frequency="Daily",
                regulatory_citations=["TEST-REG-002"],
                risk_objective="Test simple population counting",
            ),
            ontology_bindings=[
                SemanticMapping(
                    business_term="Status",
                    dataset_alias="data_sheet1",
                    technical_field="status",
                    data_type="string",
                )
            ],
            population=PopulationPipeline(
                base_dataset="data_sheet1",
                steps=[],  # No joins or filters
            ),
            assertions=[
                ValueMatchAssertion(
                    assertion_type="value_match",
                    assertion_id="check_active",
                    description="Status must be ACTIVE",
                    field="status",
                    operator="eq",
                    expected_value="ACTIVE",
                    materiality_threshold_percent=0.0,
                )
            ],
            evidence=EvidenceRequirements(
                retention_years=7,
                reviewer_workflow="Auto-Close_If_Pass",
                exception_routing_queue="test_queue",
            ),
        )

        engine = ExecutionEngine()

        try:
            compiler = ControlCompiler(dsl)
            population_count = engine._get_population_count(manifests, dsl, compiler)

            # Expected: 5 (all rows)
            assert population_count == 5, f"Expected 5, got {population_count}"

            print(f"✅ Simple population count correct: {population_count}")

        finally:
            engine.close()


if __name__ == "__main__":
    print("=" * 80)
    print("Testing Population Count Fix")
    print("=" * 80)

    print("\nTest 1: Population count with JOIN and joined column filter")
    print("-" * 80)
    test_population_count_with_join()

    print("\nTest 2: Population count without JOIN (baseline)")
    print("-" * 80)
    test_population_count_without_join()

    print("\n" + "=" * 80)
    print("✅ All tests passed!")
    print("=" * 80)
