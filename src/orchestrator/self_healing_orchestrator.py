"""
Self-Healing Orchestrator Module
Single Responsibility: Manage AI-powered self-healing loop
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from src.ai.translator import AITranslator
from src.compiler.sql_compiler import ControlCompiler
from src.execution.engine import ExecutionEngine
from src.models.dsl import EnterpriseControlDSL
from src.storage.audit_fabric import AuditFabric
from src.utils.logging_config import get_logger

if TYPE_CHECKING:
    from src.ai.translator import MockAITranslator

logger = get_logger(__name__)


class HealingResult:
    """Data class for self-healing results"""

    def __init__(
        self, healed_dsl: EnterpriseControlDSL, healed_sql: str, success: bool
    ):
        self.healed_dsl = healed_dsl
        self.healed_sql = healed_sql
        self.success = success


class SelfHealingOrchestrator:
    """
    Manages self-healing loop when SQL validation fails.
    Feeds DuckDB errors back to AI for correction.
    """

    def __init__(
        self,
        ai_translator: Union[AITranslator, "MockAITranslator"],
        engine: ExecutionEngine,
        audit: AuditFabric,
    ):
        """
        Initialize self-healing orchestrator.

        Args:
            ai_translator: AI translator for DSL healing (can be AITranslator or MockAITranslator)
            engine: Execution engine for SQL validation
            audit: Audit fabric for saving healed DSL
        """
        self.ai = ai_translator
        self.engine = engine
        self.audit = audit
        logger.debug("SelfHealingOrchestrator initialized")

    def attempt_healing(
        self,
        dsl: EnterpriseControlDSL,
        error_msg: str,
        headers: Dict[str, List[str]],
        manifests: Dict[str, Dict[str, Any]],
    ) -> Optional[HealingResult]:
        """
        Attempts to heal invalid SQL by feeding error back to AI.

        Args:
            dsl: Original DSL that produced invalid SQL
            error_msg: DuckDB error message
            headers: Column headers for re-validation
            manifests: Parquet manifests for SQL compilation

        Returns:
            HealingResult with healed DSL/SQL, or None if healing failed
        """
        logger.info(
            f"Attempting AI self-healing for control {dsl.governance.control_id}"
        )
        logger.debug(f"Error message: {error_msg[:200]}")

        try:
            # Feed error to AI for correction
            healed_dsl = self.ai.heal_dsl(dsl, error_msg, headers)

            logger.info("AI healing completed, recompiling SQL")

            # Recompile and re-validate
            compiler = ControlCompiler(healed_dsl)
            healed_sql = compiler.compile_to_sql(manifests)
            is_valid, new_error_msg = self.engine.validate_sql_dry_run(healed_sql)

            if not is_valid:
                logger.error(
                    f"Self-healing failed for {dsl.governance.control_id}. "
                    f"Persistent error: {new_error_msg}"
                )
                return None

            logger.info(f"Self-healing successful for {dsl.governance.control_id}")

            # Save healed DSL to audit database
            self.audit.save_control(
                healed_dsl.model_dump(), approved_by="AI_SELF_HEALED_SYSTEM"
            )

            return HealingResult(
                healed_dsl=healed_dsl, healed_sql=healed_sql, success=True
            )

        except Exception as e:
            logger.error(
                f"AI self-healing crashed for {dsl.governance.control_id}: {e}",
                exc_info=True,
            )
            return None
