"""
Comprehensive test suite for all fixes from Gemini review
Tests NULL handling, composite joins, new assertion types, etc.
"""

import pytest

from src.compiler.sql_compiler import ControlCompiler
from src.models.dsl import EnterpriseControlDSL


def test_null_comparison_is_null():
    """Test NULL comparison generates IS NULL instead of = NULL"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-NULL-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test NULL handling",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Check for NULL values",
                "field": "approval_date",
                "operator": "eq",
                "expected_value": None,
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

    # Should use IS NULL, not = NULL
    assert "approval_date IS NULL" in sql
    assert "= NULL" not in sql


def test_null_comparison_is_not_null():
    """Test NOT NULL comparison generates IS NOT NULL"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-NULL-002",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test NOT NULL handling",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Check for non-NULL values",
                "field": "approval_date",
                "operator": "neq",
                "expected_value": None,
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

    # Should use IS NOT NULL, not != NULL
    assert "approval_date IS NOT NULL" in sql
    assert "!= NULL" not in sql


def test_temporal_date_math_assertion():
    """Test TemporalDateMathAssertion generates INTERVAL arithmetic"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-DATE-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test date math",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "temporal_date_math",
                "description": "EDD must complete within 14 days",
                "base_date_field": "edd_completion_date",
                "operator": "lte",
                "target_date_field": "onboarding_date",
                "offset_days": 14,
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

    # Should generate proper INTERVAL syntax (with or without CAST for type safety)
    assert "+ INTERVAL 14 DAY" in sql
    assert "edd_completion_date" in sql
    assert "onboarding_date" in sql
    # Should NOT have string literal date math
    assert "'onboarding_date + 14 days'" not in sql


def test_column_comparison_assertion():
    """Test ColumnComparisonAssertion compares two columns"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-COL-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test column comparison",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "column_comparison",
                "description": "Trade date must be after clearance date",
                "left_field": "trade_date",
                "operator": "gt",
                "right_field": "clearance_date",
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

    # Should compare columns directly without quoting
    assert "trade_date > clearance_date" in sql
    # Should NOT quote the second column as a string
    assert "'clearance_date'" not in sql


def test_composite_join():
    """Test JoinLeft supports multiple keys for composite joins"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-JOIN-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test composite join",
        },
        "ontology_bindings": [],
        "population": {
            "base_dataset": "trades",
            "steps": [
                {
                    "step_id": "join_restrictions",
                    "action": {
                        "operation": "join_left",
                        "left_dataset": "trades",
                        "right_dataset": "restrictions",
                        "left_keys": ["employee_id", "ticker_symbol"],
                        "right_keys": ["employee_id", "ticker_symbol"],
                    },
                }
            ],
        },
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Must be cleared",
                "field": "status",
                "operator": "eq",
                "expected_value": "CLEARED",
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
        "restrictions": {
            "parquet_path": "/tmp/restrictions.parquet",
            "sha256_hash": "def456",
        },
    }
    sql = compiler.compile_to_sql(manifests)

    # Should have composite join condition with AND
    assert "base.employee_id = right_tbl.employee_id" in sql
    assert "base.ticker_symbol = right_tbl.ticker_symbol" in sql
    assert "AND" in sql


def test_generalized_aggregation_count():
    """Test AggregationAssertion supports COUNT function"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-AGG-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test COUNT aggregation",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "aggregation",
                "description": "Max 50 trades per trader per day",
                "group_by_fields": ["trader_id", "trade_date"],
                "metric_field": "trade_id",
                "aggregation_function": "COUNT",
                "operator": "lte",
                "threshold": 50,
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

    # Should use COUNT function, not SUM
    assert "COUNT(trade_id)" in sql
    assert "GROUP BY" in sql
    assert "trader_id" in sql
    assert "trade_date" in sql


def test_case_insensitive_string_comparison():
    """Test case-insensitive string handling with TRIM and UPPER"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-CASE-001",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test case-insensitive comparison",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Status must be approved",
                "field": "status",
                "operator": "eq",
                "expected_value": "APPROVED",
                "ignore_case_and_space": True,
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

    # Should use TRIM and UPPER for case-insensitive comparison
    assert "TRIM(UPPER(CAST(status AS VARCHAR)))" in sql
    assert "TRIM(UPPER('APPROVED'))" in sql


def test_case_sensitive_when_disabled():
    """Test case-sensitive comparison when ignore_case_and_space is False"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-CASE-002",
            "version": "1.0.0",
            "owner_role": "Test",
            "testing_frequency": "Daily",
            "regulatory_citations": ["TEST"],
            "risk_objective": "Test case-sensitive comparison",
        },
        "ontology_bindings": [],
        "population": {"base_dataset": "test_data", "steps": []},
        "assertions": [
            {
                "assertion_id": "assert_001",
                "assertion_type": "value_match",
                "description": "Status must be exactly APPROVED",
                "field": "status",
                "operator": "eq",
                "expected_value": "APPROVED",
                "ignore_case_and_space": False,
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

    # Should NOT use TRIM and UPPER when disabled
    assert "TRIM(UPPER(" not in sql
    assert "status = 'APPROVED'" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
