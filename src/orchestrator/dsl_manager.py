"""
DSL Manager Module
Single Responsibility: Manage DSL retrieval, generation, and caching
"""

from typing import TYPE_CHECKING, Dict, List, Optional, Union

from src.ai.translator import AITranslator
from src.execution.ingestion import EvidenceIngestion
from src.models.dsl import EnterpriseControlDSL
from src.storage.audit_fabric import AuditFabric
from src.utils.logging_config import get_logger

if TYPE_CHECKING:
    from src.ai.translator import MockAITranslator

logger = get_logger(__name__)


class DSLResult:
    """Data class for DSL retrieval/generation results"""

    def __init__(
        self,
        dsl: EnterpriseControlDSL,
        was_cached: bool,
        headers: Optional[Dict[str, List[str]]] = None,
    ):
        self.dsl = dsl
        self.was_cached = was_cached
        self.headers = headers or {}


class DSLManager:
    """
    Manages DSL lifecycle: retrieval from cache, generation via AI, and persistence.
    Separates DSL concerns from orchestration logic.
    """

    def __init__(
        self, ai_translator: Union[AITranslator, "MockAITranslator"], audit: AuditFabric
    ):
        """
        Initialize DSL manager.

        Args:
            ai_translator: AI translator for DSL generation (can be AITranslator or MockAITranslator)
            audit: Audit fabric for DSL persistence
        """
        self.ai = ai_translator
        self.audit = audit
        logger.debug("DSLManager initialized")

    def get_or_generate_dsl(
        self,
        control_id: str,
        control_text: str,
        excel_files: List,
        ingestion: EvidenceIngestion,
    ) -> DSLResult:
        """
        Retrieves DSL from cache or generates it via AI.

        Args:
            control_id: Control identifier
            control_text: Control procedure text
            excel_files: List of Excel file paths
            ingestion: Evidence ingestion instance for header extraction

        Returns:
            DSLResult with DSL, cache status, and headers

        Raises:
            ValueError: If DSL generation fails
        """
        logger.debug(f"Checking for cached DSL: {control_id}")

        # Try to retrieve from cache
        dsl_dict = self.audit.get_control(control_id)

        if dsl_dict:
            logger.info(
                f"DSL found in cache for {control_id}, version {dsl_dict['governance']['version']}"
            )
            dsl = EnterpriseControlDSL(**dsl_dict)

            # Extract headers for potential self-healing
            headers = self._extract_headers(excel_files, ingestion)

            return DSLResult(dsl=dsl, was_cached=True, headers=headers)

        # Generate new DSL
        logger.info(f"DSL not found for {control_id}, triggering AI generation")

        # Extract headers for schema pruning
        headers = self._extract_headers(excel_files, ingestion)

        if not headers:
            logger.error("No headers could be extracted from any Excel files")
            raise ValueError("No headers could be extracted from Excel files")

        # Generate DSL via AI
        logger.info(f"Calling AI translator for control {control_id}")
        dsl = self.ai.translate_control(control_text, headers)

        # Override control_id if AI hallucinated
        dsl.governance.control_id = control_id

        # Save to cache
        logger.debug(f"Saving generated DSL to audit database for {control_id}")
        self.audit.save_control(dsl.model_dump(), approved_by="AUTO_GENERATED_SYSTEM")

        logger.info(
            f"DSL generated and saved for {control_id}, version {dsl.governance.version}"
        )

        return DSLResult(dsl=dsl, was_cached=False, headers=headers)

    @staticmethod
    def _extract_headers(
        excel_files: List, ingestion: EvidenceIngestion
    ) -> Dict[str, List[str]]:
        """
        Extracts column headers from Excel files.

        Args:
            excel_files: List of Excel file paths
            ingestion: Evidence ingestion instance

        Returns:
            Dictionary mapping dataset_alias to column list
        """
        headers = {}

        for excel in excel_files:
            try:
                sheet_headers = ingestion.get_column_headers(str(excel))
                for sheet_name, cols in sheet_headers.items():
                    dataset_alias = f"{excel.stem}_{sheet_name}".lower()
                    headers[dataset_alias] = cols
                    logger.debug(f"Extracted {len(cols)} columns from {dataset_alias}")
            except Exception as e:
                logger.warning(f"Failed to extract headers from {excel.name}: {e}")

        return headers
