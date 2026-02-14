"""
Domain Specific Language (DSL) for Compliance Controls
Pydantic v2 models with discriminated unions for type safety
"""

from datetime import datetime
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

# ==========================================
# DOMAIN 1: GOVERNANCE & ONTOLOGY
# ==========================================


class ControlGovernance(BaseModel):
    """Metadata linking control to regulatory frameworks and ownership"""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    control_id: str
    version: str
    owner_role: str
    testing_frequency: Literal["Continuous", "Daily", "Weekly", "Quarterly", "Annual"]
    regulatory_citations: List[str]
    risk_objective: str


class SemanticMapping(BaseModel):
    """Maps business terminology to physical evidence columns"""

    model_config = ConfigDict(extra="forbid")

    business_term: str
    dataset_alias: str
    technical_field: str
    data_type: Literal["string", "numeric", "timestamp", "boolean", "date"]


# ==========================================
# DOMAIN 3: POPULATION PIPELINE (DISCRIMINATED UNIONS)
# ==========================================


class FilterComparison(BaseModel):
    """Filter rows based on comparison operator"""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["filter_comparison"] = "filter_comparison"
    field: str
    operator: Literal["eq", "neq", "gt", "lt", "gte", "lte"]
    value: Union[str, int, float, datetime]


class FilterInList(BaseModel):
    """Filter rows based on value list membership"""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["filter_in_list"] = "filter_in_list"
    field: str
    values: List[Union[str, int, float]]


class JoinLeft(BaseModel):
    """Left join two datasets"""

    model_config = ConfigDict(extra="forbid")

    operation: Literal["join_left"] = "join_left"
    left_dataset: str
    right_dataset: str
    left_key: str
    right_key: str


# Unified Pipeline Action (Discriminated Union with proper Pydantic v2 syntax)
PipelineAction = Annotated[
    Union[FilterComparison, FilterInList, JoinLeft], Field(discriminator="operation")
]


class PopulationPipelineStep(BaseModel):
    """Single step in population derivation pipeline"""

    model_config = ConfigDict(extra="forbid")

    step_id: str
    action: PipelineAction  # Discriminator is now part of PipelineAction type


class SamplingStrategy(BaseModel):
    """SOX-compliant sampling methodology"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    method: Literal["random", "stratified", "systematic", "judgmental"] = "random"
    sample_size: Optional[int] = None
    sample_percentage: Optional[float] = None
    stratification_field: Optional[str] = None
    random_seed: Optional[int] = 42
    justification: str = ""


class PopulationPipeline(BaseModel):
    """Defines population derivation logic"""

    model_config = ConfigDict(extra="forbid")

    base_dataset: str
    steps: List[PopulationPipelineStep]
    sampling: Optional[SamplingStrategy] = None


# ==========================================
# DOMAIN 4: ASSERTION TAXONOMY (DISCRIMINATED UNIONS)
# ==========================================


class BaseAssertion(BaseModel):
    """Base class for all assertions"""

    model_config = ConfigDict(extra="forbid")

    assertion_id: str
    description: str
    materiality_threshold_percent: float = 0.0


class ValueMatchAssertion(BaseAssertion):
    """Row-level value comparison assertion"""

    assertion_type: Literal["value_match"] = "value_match"
    field: str
    operator: Literal["eq", "neq", "gt", "lt", "gte", "lte", "in", "not_in"]
    expected_value: Union[str, int, float, List[str], List[int]]


class TemporalSequenceAssertion(BaseAssertion):
    """Temporal ordering assertion"""

    assertion_type: Literal["temporal_sequence"] = "temporal_sequence"
    event_chain: List[str]


class AggregationSumAssertion(BaseAssertion):
    """Aggregation-level assertion"""

    assertion_type: Literal["aggregation_sum"] = "aggregation_sum"
    group_by_fields: List[str]
    metric_field: str
    operator: Literal["gt", "lt", "eq", "gte", "lte"]
    threshold: float


# Unified Assertion Type (Discriminated Union with proper Pydantic v2 syntax)
Assertion = Annotated[
    Union[ValueMatchAssertion, TemporalSequenceAssertion, AggregationSumAssertion],
    Field(discriminator="assertion_type"),
]


# ==========================================
# DOMAIN 5 & MASTER SCHEMA
# ==========================================


class EvidenceRequirements(BaseModel):
    """Audit retention and exception routing configuration"""

    model_config = ConfigDict(extra="forbid")

    retention_years: int = 7
    reviewer_workflow: Literal[
        "Auto-Close_If_Pass", "Requires_Human_Signoff", "Four_Eyes_Review"
    ]
    exception_routing_queue: str


class EnterpriseControlDSL(BaseModel):
    """
    The canonical representation of a compliance control.
    Once approved, this JSON becomes immutable and versioned.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    governance: ControlGovernance
    ontology_bindings: List[SemanticMapping]
    population: PopulationPipeline
    assertions: List[Assertion]  # Discriminator is now part of Assertion type
    evidence: EvidenceRequirements
