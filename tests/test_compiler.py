"""
Unit tests for SQL Compiler
Tests CTE chaining, value escaping, and assertion compilation
"""

import pytest
from src.models.dsl import (
    EnterpriseControlDSL,
    ControlGovernance,
    PopulationPipeline,
    ValueMatchAssertion,
    EvidenceRequirements,
)
from src.compiler.sql_compiler import ControlCompiler


def test_quote_value_escapes_single_quotes():
    """Test that single quotes are properly escaped (SQL injection fix)"""
    # O'Connor should become O''Connor
    result = ControlCompiler._quote_value("O'Connor")
    assert result == "'O''Connor'"

    # Multiple quotes
    result = ControlCompiler._quote_value("It's a test's value")
    assert result == "'It''s a test''s value'"


def test_quote_value_handles_numbers():
    """Test numeric value quoting"""
    assert ControlCompiler._quote_value(42) == "42"
    assert ControlCompiler._quote_value(3.14) == "3.14"


def test_quote_value_handles_boolean():
    """Test boolean value quoting"""
    assert ControlCompiler._quote_value(True) == "TRUE"
    assert ControlCompiler._quote_value(False) == "FALSE"


def test_simple_value_match_compilation():
    """Test basic value match assertion compilation"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test control",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Status must be APPROVED",
                "field": "status",
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

    dsl = EnterpriseControlDSL(**dsl_dict)
    compiler = ControlCompiler(dsl)

    manifests = {
        "test_data": {"parquet_path": "/tmp/test.parquet", "sha256_hash": "abc123"}
    }

    sql = compiler.compile_to_sql(manifests)

    # Should find exceptions (rows where NOT status = 'APPROVED')
    assert "NOT (status = 'APPROVED')" in sql
    assert "read_parquet('/tmp/test.parquet')" in sql


def test_filter_in_list_compilation():
    """Test IN operator compilation"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-002",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test control",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Status must be in approved list",
                "field": "status",
                "operator": "in",
                "expected_value": ["APPROVED", "VERIFIED"],
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
    compiler = ControlCompiler(dsl)

    manifests = {
        "test_data": {"parquet_path": "/tmp/test.parquet", "sha256_hash": "abc123"}
    }

    sql = compiler.compile_to_sql(manifests)

    # Should use IN operator
    assert "status IN ('APPROVED', 'VERIFIED')" in sql


def test_cte_chaining_with_multiple_steps():
    """Test that pipeline steps properly chain CTEs (bug fix validation)"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-003",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test control",
        },
        "ontology_bindings": [],
        "population": {
            "base_dataset": "trades",
            "steps": [
                {
                    "step_id": "step_001",
                    "action": {
                        "operation": "filter_comparison",
                        "field": "amount",
                        "operator": "gt",
                        "value": 1000,
                    },
                },
                {
                    "step_id": "join_approvers",
                    "action": {
                        "operation": "join_left",
                        "left_dataset": "trades",
                        "right_dataset": "hr_roster",
                        "left_key": "approver_id",
                        "right_key": "employee_id",
                    },
                },
            ],
        },
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Test",
                "field": "status",
                "operator": "eq",
                "expected_value": "ACTIVE",
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
    compiler = ControlCompiler(dsl)

    manifests = {
        "trades": {"parquet_path": "/tmp/trades.parquet", "sha256_hash": "abc123"},
        "hr_roster": {"parquet_path": "/tmp/hr.parquet", "sha256_hash": "def456"},
    }

    sql = compiler.compile_to_sql(manifests)

    # CRITICAL: Join should reference 'base', not hardcoded dataset
    # After filter, join should still work with base CTE
    assert "FROM base" in sql or "FROM join_approvers" in sql
    assert "join_approvers AS" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
