"""
Orchestrator Module (BACKWARD COMPATIBILITY STUB)

This file is maintained for backward compatibility with existing imports.
The orchestrator has been refactored into a modular architecture.

NEW USAGE:
    from src.orchestrator import BatchOrchestrator

MODULAR COMPONENTS (located in src/orchestrator/):
    - project_reader.py         : Reads project metadata and discovers evidence
    - dsl_manager.py            : Manages DSL retrieval, generation, and caching
    - validation_orchestrator.py: Coordinates LLM validation (optional)
    - self_healing_orchestrator.py: Manages AI-powered self-healing loop
    - execution_orchestrator.py : Coordinates SQL compilation and execution
    - result_formatter.py       : Formats results and summaries
    - batch_orchestrator.py     : Main coordinator (simplified from 679 to ~400 lines)

BENEFITS OF MODULARIZATION:
    ✓ Single Responsibility Principle adhered
    ✓ Each module has one clear purpose
    ✓ Easy to test individual components
    ✓ Clean separation of concerns
    ✓ Consistent error handling
    ✓ Better maintainability and extensibility

ORIGINAL ISSUES RESOLVED:
    ✗ Monolithic _process_single_project (400+ lines)
    ✗ Mixed responsibilities (file I/O, DSL, validation, execution, formatting)
    ✗ Difficult to test
    ✗ Instance variable state management
    ✗ Inconsistent exception handling
"""

# Re-export for backward compatibility
from src.orchestrator.batch_orchestrator import BatchOrchestrator

__all__ = ["BatchOrchestrator"]
