"""
Integration test for end-to-end control execution
Creates test Excel data, ingests it, and executes a control
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from src.models.dsl import EnterpriseControlDSL
from src.execution.ingestion import EvidenceIngestion
from src.execution.engine import ExecutionEngine
from src.storage.audit_fabric import AuditFabric


@pytest.fixture
def sample_excel_file():
    """Creates a temporary Excel file for testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        excel_path = Path(tmpdir) / "test_trades.xlsx"

        # Create sample data
        df = pd.DataFrame(
            {
                "trade_id": ["T001", "T002", "T003", "T004"],
                "amount": [5000, 15000, 25000, 8000],
                "approval_status": ["APPROVED", "APPROVED", "PENDING", "APPROVED"],
                "trade_date": ["2025-07-01", "2025-07-15", "2025-08-01", "2025-09-01"],
            }
        )

        df.to_excel(excel_path, index=False, sheet_name="trades")
        yield str(excel_path)


@pytest.fixture
def sample_dsl():
    """Creates a sample DSL for testing"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-E2E-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Ensure all trades are approved",
        },
        "ontology_bindings": [
            {
                "business_term": "Trade Amount",
                "dataset_alias": "test_trades_trades",
                "technical_field": "amount",
                "data_type": "numeric",
            },
            {
                "business_term": "Approval Status",
                "dataset_alias": "test_trades_trades",
                "technical_field": "approval_status",
                "data_type": "string",
            },
        ],
        "population": {
            "base_dataset": "test_trades_trades",
            "steps": [
                {
                    "step_id": "filter_large",
                    "action": {
                        "operation": "filter_comparison",
                        "field": "amount",
                        "operator": "gt",
                        "value": 10000,
                    },
                }
            ],
        },
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "All large trades must be approved",
                "field": "approval_status",
                "operator": "eq",
                "expected_value": "APPROVED",
                "materiality_threshold_percent": 0.0,
            }
        ],
        "evidence": {
            "retention_years": 7,
            "reviewer_workflow": "Auto-Close_If_Pass",
            "exception_routing_queue": "TEST",
        },
    }

    return EnterpriseControlDSL(**dsl_dict)


def test_end_to_end_control_execution(sample_excel_file, sample_dsl):
    """Test complete workflow: ingest → execute → audit"""

    with tempfile.TemporaryDirectory() as tmpdir:
        # Initialize components
        ingestion = EvidenceIngestion(storage_dir=tmpdir)
        engine = ExecutionEngine()
        audit = AuditFabric(db_path=f"{tmpdir}/test_audit.db")

        # Step 1: Ingest Excel to Parquet
        manifests = ingestion.ingest_excel_to_parquet(
            excel_path=sample_excel_file,
            dataset_prefix="test_trades",
            source_system="TEST_SYSTEM",
        )

        assert len(manifests) == 1
        assert manifests[0]["dataset_alias"] == "test_trades_trades"
        assert manifests[0]["row_count"] == 4
        assert len(manifests[0]["sha256_hash"]) == 64  # SHA-256 length

        # Step 2: Execute control
        manifest_dict = {m["dataset_alias"]: m for m in manifests}
        report = engine.execute_control(sample_dsl, manifest_dict)

        # Validate results
        assert report["control_id"] == "TEST-E2E-001"
        assert report["verdict"] in ["PASS", "FAIL"]
        assert "exception_count" in report
        assert "total_population" in report

        # Expected: 2 large trades (>10k), 1 is PENDING (exception)
        assert report["exception_count"] == 1
        assert report["total_population"] >= 2

        # Step 3: Save to audit ledger
        audit.save_control(sample_dsl.model_dump(), approved_by="test@example.com")
        audit.save_execution(report)

        # Step 4: Verify audit trail
        control = audit.get_control("TEST-E2E-001")
        assert control is not None
        assert control["governance"]["control_id"] == "TEST-E2E-001"

        history = audit.get_execution_history("TEST-E2E-001")
        assert len(history) == 1
        assert history[0]["verdict"] == report["verdict"]

        # Cleanup
        engine.close()
        audit.close()


def test_schema_validation():
    """Test pre-flight schema validation"""

    dsl_dict = {
        "governance": {
            "control_id": "TEST-SCHEMA-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test",
        },
        "ontology_bindings": [
            {
                "business_term": "Missing Column",
                "dataset_alias": "test_data",
                "technical_field": "nonexistent_column",
                "data_type": "string",
            }
        ],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Test",
                "field": "status",
                "operator": "eq",
                "expected_value": "OK",
                "materiality_threshold_percent": 0.0,
            }
        ],
        "evidence": {
            "retention_years": 7,
            "reviewer_workflow": "Auto-Close_If_Pass",
            "exception_routing_queue": "TEST",
        },
    }

    dsl = EnterpriseControlDSL(**dsl_dict)
    engine = ExecutionEngine()

    # Mock manifest with different columns
    manifests = {
        "test_data": {
            "parquet_path": "/tmp/test.parquet",
            "sha256_hash": "abc123",
            "columns": ["status", "amount", "date"],
        }
    }

    validation = engine.validate_schema(manifests, dsl)

    # Should detect schema drift
    assert validation["overall_status"] == "SCHEMA_DRIFT_DETECTED"
    assert (
        "nonexistent_column" in validation["dataset_validations"][0]["missing_columns"]
    )

    engine.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
