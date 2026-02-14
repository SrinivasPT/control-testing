"""
Unit tests for Pydantic DSL models
Tests discriminated unions and validation
"""

import pytest
from pydantic import ValidationError
from src.models.dsl import (
    EnterpriseControlDSL,
    FilterComparison,
    JoinLeft,
    ValueMatchAssertion,
    AggregationSumAssertion,
)


def test_filter_comparison_discriminated_union():
    """Test that discriminated union works for FilterComparison"""
    action = FilterComparison(
        operation="filter_comparison", field="amount", operator="gt", value=1000
    )

    assert action.operation == "filter_comparison"
    assert action.field == "amount"


def test_invalid_operator_raises_error():
    """Test that invalid operator is rejected (prevents hallucination)"""
    with pytest.raises(ValidationError):
        FilterComparison(
            operation="filter_comparison",
            field="amount",
            operator="fuzzy_match",  # Invalid operator
            value=1000,
        )


def test_join_left_discriminated_union():
    """Test JoinLeft action"""
    action = JoinLeft(
        operation="join_left",
        left_dataset="trades",
        right_dataset="hr",
        left_key="approver_id",
        right_key="employee_id",
    )

    assert action.operation == "join_left"


def test_value_match_assertion():
    """Test ValueMatchAssertion creation"""
    assertion = ValueMatchAssertion(
        assertion_id="assert_001",
        assertion_type="value_match",
        description="Test",
        field="status",
        operator="eq",
        expected_value="APPROVED",
        materiality_threshold_percent=0.0,
    )

    assert assertion.assertion_type == "value_match"


def test_aggregation_sum_assertion():
    """Test AggregationSumAssertion creation"""
    assertion = AggregationSumAssertion(
        assertion_id="assert_002",
        assertion_type="aggregation_sum",
        description="Test",
        group_by_fields=["trader_id"],
        metric_field="notional_amount",
        operator="gt",
        threshold=1000000.0,
        materiality_threshold_percent=5.0,
    )

    assert assertion.assertion_type == "aggregation_sum"
    assert assertion.threshold == 1000000.0


def test_complete_dsl_validation():
    """Test complete DSL validation"""
    dsl_dict = {
        "governance": {
            "control_id": "TEST-001",
            "version": "1.0.0",
            "owner_role": "Test Owner",
            "testing_frequency": "Daily",
            "regulatory_citations": ["SOX 404"],
            "risk_objective": "Test objective",
        },
        "ontology_bindings": [
            {
                "business_term": "Trade Amount",
                "dataset_alias": "trades",
                "technical_field": "notional_usd",
                "data_type": "numeric",
            }
        ],
        "population": {
            "base_dataset": "trades",
            "steps": [
                {
                    "step_id": "filter_001",
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
                "description": "Status must be approved",
                "field": "approval_status",
                "operator": "eq",
                "expected_value": "APPROVED",
                "materiality_threshold_percent": 0.0,
            }
        ],
        "evidence": {
            "retention_years": 7,
            "reviewer_workflow": "Requires_Human_Signoff",
            "exception_routing_queue": "JIRA:COMPLIANCE",
        },
    }

    # Should not raise any errors
    dsl = EnterpriseControlDSL(**dsl_dict)

    assert dsl.governance.control_id == "TEST-001"
    assert len(dsl.assertions) == 1
    assert dsl.assertions[0].assertion_type == "value_match"


def test_extra_fields_forbidden():
    """Test that extra fields are rejected (strict validation)"""
    with pytest.raises(ValidationError):
        EnterpriseControlDSL(
            governance={
                "control_id": "TEST-001",
                "version": "1.0.0",
                "owner_role": "Test",
                "testing_frequency": "Daily",
                "regulatory_citations": [],
                "risk_objective": "Test",
                "extra_field": "not allowed",  # Should fail
            },
            ontology_bindings=[],
            population={"base_dataset": "test", "steps": []},
            assertions=[],
            evidence={
                "retention_years": 7,
                "reviewer_workflow": "Auto-Close_If_Pass",
                "exception_routing_queue": "TEST",
            },
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
