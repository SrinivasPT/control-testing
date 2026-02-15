"""
Batch Orchestrator Module
Coordinates all single-responsibility orchestration modules
"""

import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from src.ai.translator import AITranslator
from src.ai.validator import AIValidator
from src.execution.engine import ExecutionEngine
from src.execution.ingestion import EvidenceIngestion
from src.orchestrator.dsl_manager import DSLManager
from src.orchestrator.execution_orchestrator import ExecutionOrchestrator
from src.orchestrator.project_reader import ProjectReader
from src.orchestrator.result_formatter import ResultFormatter
from src.orchestrator.self_healing_orchestrator import SelfHealingOrchestrator
from src.orchestrator.validation_orchestrator import ValidationOrchestrator
from src.storage.audit_fabric import AuditFabric
from src.utils.logging_config import get_logger

# Load environment variables
load_dotenv()

# Get logger for this module
logger = get_logger(__name__)


class BatchOrchestrator:
    """
    Orchestrates end-to-end control testing for multiple projects.
    Coordinates single-responsibility modules for clean separation of concerns.

    Workflow:
    1. Discover projects
    2. For each project:
       a. Read project metadata (ProjectReader)
       b. Get/generate DSL (DSLManager)
       c. Ingest evidence (EvidenceIngestion)
       d. Validate (ValidationOrchestrator - optional)
       e. Compile & validate SQL (ExecutionOrchestrator)
       f. Self-heal if needed (SelfHealingOrchestrator)
       g. Execute test (ExecutionOrchestrator)
       h. Format results (ResultFormatter)
    3. Generate summary report (ResultFormatter)
    """

    def __init__(
        self,
        use_mock_ai: bool = False,
        db_path: str = "data/audit.db",
        parquet_dir: str = "data/parquet",
        enable_llm_validation: bool = False,
    ):
        """
        Initialize orchestrator with all layer dependencies.

        Args:
            use_mock_ai: If True, skip real LLM calls (for testing)
            db_path: Path to SQLite audit database
            parquet_dir: Directory for Parquet storage
            enable_llm_validation: If True, use LLM to validate DSL and SQL before execution
        """
        logger.info("Initializing BatchOrchestrator")
        logger.debug(
            f"Configuration: use_mock_ai={use_mock_ai}, db_path={db_path}, "
            f"parquet_dir={parquet_dir}, enable_llm_validation={enable_llm_validation}"
        )

        # Initialize base components
        self._init_ai_components(use_mock_ai, enable_llm_validation)
        self._init_execution_components(db_path, parquet_dir)
        self._init_orchestration_modules()

        logger.info("BatchOrchestrator initialization complete")

    def _init_ai_components(self, use_mock_ai: bool, enable_llm_validation: bool):
        """Initialize AI translation and validation components"""
        if use_mock_ai:
            from src.ai.translator import MockAITranslator

            logger.info("Using MockAITranslator (no API calls)")
            self.ai = MockAITranslator()
            self.validator = None
        else:
            api_key = os.getenv("DEEPSEEK_API_KEY")
            if not api_key:
                logger.error("DEEPSEEK_API_KEY not found in environment")
                raise ValueError(
                    "DEEPSEEK_API_KEY not found. Set in .env or use use_mock_ai=True"
                )
            logger.info("Initializing AITranslator with DeepSeek API")
            self.ai = AITranslator(api_key=api_key)

            if enable_llm_validation:
                logger.info("Initializing AIValidator for DSL/SQL validation")
                self.validator = AIValidator(api_key=api_key)
            else:
                logger.debug("LLM validation disabled (performance mode)")
                self.validator = None

    def _init_execution_components(self, db_path: str, parquet_dir: str):
        """Initialize execution and persistence components"""
        logger.debug("Initializing execution components")
        self.ingestion = EvidenceIngestion(storage_dir=parquet_dir)
        self.engine = ExecutionEngine()
        self.audit = AuditFabric(db_path=db_path)

    def _init_orchestration_modules(self):
        """Initialize single-responsibility orchestration modules"""
        logger.debug("Initializing orchestration modules")
        self.dsl_manager = DSLManager(self.ai, self.audit)
        self.validation_orchestrator = ValidationOrchestrator(self.validator)
        self.self_healing_orchestrator = SelfHealingOrchestrator(
            self.ai, self.engine, self.audit
        )
        self.execution_orchestrator = ExecutionOrchestrator(self.engine)

    def process_all_projects(self, input_dir: str = "data/input") -> Dict[str, Any]:
        """
        Main entry point: processes all project folders in input directory.

        Args:
            input_dir: Base directory containing project folders

        Returns:
            Summary report with overall statistics
        """
        logger.info(f"Starting batch processing from directory: {input_dir}")

        # Discover projects
        try:
            project_folders = ProjectReader.discover_projects(input_dir)
        except FileNotFoundError as e:
            logger.error(str(e))
            print(f"⚠️  {e}")
            return {"total_projects": 0, "results": []}

        if not project_folders:
            logger.warning(f"No project folders found in {input_dir}")
            print(f"⚠️  No project folders found in {input_dir}")
            return {"total_projects": 0, "results": []}

        # Print header
        self._print_batch_header(len(project_folders))

        # Process each project
        results = []
        for project_path in project_folders:
            project_name = project_path.name
            logger.info(f"Starting processing for project: {project_name}")
            self._print_project_header(project_name)

            result = self._process_single_project(project_path)
            results.append(result)

            logger.info(
                f"Project {project_name} completed with verdict: {result['verdict']}"
            )
            ResultFormatter.print_project_result(result)

        # Generate and print summary
        summary = ResultFormatter.generate_summary(results)
        ResultFormatter.print_summary(summary)

        return summary

    def _process_single_project(self, project_path: Path) -> Dict[str, Any]:
        """
        Processes a single project through the full pipeline.
        Now significantly simplified by delegating to specialized modules.

        Returns:
            Execution result dictionary
        """
        project_name = project_path.name

        try:
            # Step 1: Read project metadata
            print("\n[1/5] 📄 Reading control-information.md...")
            project_info = ProjectReader.read_project(project_path)

            if project_info is None:
                # No Excel files found
                return ResultFormatter.format_project_result(
                    project_name=project_name,
                    control_id="UNKNOWN",
                    verdict="SKIPPED",
                    reason="No Excel evidence files found",
                )

            print(f"   Control ID: {project_info.control_id}")

            # Step 2: Scan Excel files (already done by ProjectReader)
            print("\n[2/5] 📊 Scanning for Excel evidence files...")
            print(
                f"   Found {len(project_info.excel_files)} file(s): "
                f"{', '.join(f.name for f in project_info.excel_files)}"
            )

            # Step 3: Get or generate DSL
            print("\n[3/5] 🧠 Checking for existing DSL in audit database...")
            dsl_result = self.dsl_manager.get_or_generate_dsl(
                control_id=project_info.control_id,
                control_text=project_info.control_text,
                excel_files=project_info.excel_files,
                ingestion=self.ingestion,
            )

            if dsl_result.was_cached:
                print(
                    f"   ✓ DSL found (version {dsl_result.dsl.governance.version}) - reusing cached version"
                )
            else:
                print(
                    f"   ✓ DSL generated and saved (version {dsl_result.dsl.governance.version})"
                )

            # Step 4: Ingest evidence
            print(
                "\n[4/5] 🔄 Ingesting Excel files to Parquet with cryptographic hashing..."
            )
            manifests = self._ingest_evidence(project_info.excel_files, project_name)

            # Step 5: Optional LLM validation
            if self.validation_orchestrator.enabled:
                print("\n[5/7] 🔍 LLM Pre-Flight Validation (DSL & SQL Review)...")
                # Compile SQL for validation
                sql, _, _ = self.execution_orchestrator.compile_and_validate(
                    dsl_result.dsl, manifests
                )
                validation_result = self.validation_orchestrator.validate(
                    project_info.control_text, dsl_result.dsl, sql, manifests
                )
                if validation_result:
                    self._print_validation_result(validation_result)

            # Step 6: SQL validation & self-healing
            step_num = "6/7" if self.validation_orchestrator.enabled else "5/6"
            print(f"\n[{step_num}] ✅ DuckDB EXPLAIN Validation (Strict Judge)...")

            sql, is_valid, error_msg = self.execution_orchestrator.compile_and_validate(
                dsl_result.dsl, manifests
            )

            if not is_valid:
                print(f"   ⚠️  SQL validation failed: {error_msg[:100]}...")
                print("   🔧 Triggering AI Self-Healing protocol...")

                healing_result = self.self_healing_orchestrator.attempt_healing(
                    dsl_result.dsl, error_msg, dsl_result.headers, manifests
                )

                if not healing_result:
                    print("   ❌ Self-healing failed. SQL still invalid.")
                    return ResultFormatter.format_project_result(
                        project_name=project_name,
                        control_id=project_info.control_id,
                        verdict="ERROR",
                        error=f"Self-healing failed. Persistent SQL Error: {error_msg[:200]}",
                    )

                print("   ✓ Second validation PASSED - SQL is now correct")
                # Update DSL with healed version
                dsl_result.dsl = healing_result.healed_dsl
            else:
                print("   ✓ SQL validation PASSED - query is correct")

            # Step 7: Execute control test
            step_num = "7/7" if self.validation_orchestrator.enabled else "6/6"
            print(f"\n[{step_num}] ⚙️  Executing control via DuckDB SQL engine...")

            report = self.execution_orchestrator.execute(dsl_result.dsl, manifests)

            # Save execution to audit ledger
            self.audit.save_execution(report)
            print("   ✓ Execution complete - results saved to audit database")

            return ResultFormatter.format_project_result(
                project_name=project_name,
                control_id=project_info.control_id,
                verdict=report["verdict"],
                exception_count=report.get("exception_count", 0),
                total_population=report.get("total_population", 0),
                exception_rate=report.get("exception_rate_percent", 0),
                dsl_cached=dsl_result.was_cached,
            )

        except Exception as e:
            logger.error(
                f"Critical error processing project {project_name}: {type(e).__name__}: {str(e)}",
                exc_info=True,
            )
            print(f"\n❌ CRITICAL ERROR: {type(e).__name__}: {str(e)}")
            return ResultFormatter.format_project_result(
                project_name=project_name,
                control_id=getattr(
                    locals().get("project_info"), "control_id", "UNKNOWN"
                ),
                verdict="ERROR",
                error=str(e),
            )

    def _ingest_evidence(
        self, excel_files: List[Path], project_name: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Ingests Excel files to Parquet with cryptographic hashing.

        Args:
            excel_files: List of Excel file paths
            project_name: Project name for source tracking

        Returns:
            Dictionary mapping dataset_alias to manifest
        """
        manifests = {}

        for excel in excel_files:
            dataset_prefix = excel.stem.lower()
            logger.debug(f"Processing evidence file: {excel.name}")
            print(f"   Processing: {excel.name}...")

            manifest_list = self.ingestion.ingest_excel_to_parquet(
                str(excel), dataset_prefix, source_system=f"PROJECT_{project_name}"
            )

            for manifest in manifest_list:
                manifests[manifest["dataset_alias"]] = manifest
                # Save manifest to audit ledger
                self.audit.save_evidence_manifest(manifest)
                logger.info(
                    f"Ingested {manifest['dataset_alias']}: {manifest['row_count']} rows, "
                    f"hash: {manifest['sha256_hash'][:16]}..."
                )
                print(
                    f"      ✓ {manifest['dataset_alias']}: "
                    f"{manifest['row_count']} rows, "
                    f"hash: {manifest['sha256_hash'][:12]}..."
                )

        return manifests

    @staticmethod
    def _print_batch_header(project_count: int):
        """Prints batch processing header"""
        print(f"\n{'=' * 60}")
        print("🚀 ENTERPRISE CONTROL ORCHESTRATOR")
        print(f"{'=' * 60}")
        print(f"Found {project_count} project(s) to process\n")

    @staticmethod
    def _print_project_header(project_name: str):
        """Prints project processing header"""
        print(f"{'─' * 60}")
        print(f"📁 Processing Project: {project_name}")
        print(f"{'─' * 60}")

    @staticmethod
    def _print_validation_result(validation_result):
        """Prints LLM validation results"""
        print(
            f"   DSL Validation: {'✓ PASS' if validation_result.dsl_report.is_valid else '⚠️ ISSUES FOUND'}"
        )
        print(
            f"   SQL Validation: {'✓ PASS' if validation_result.sql_report.is_valid else '⚠️ ISSUES FOUND'}"
        )

        if validation_result.critical_issues:
            print(
                f"\n   ⚠️  {len(validation_result.critical_issues)} CRITICAL issue(s) detected:"
            )
            for idx, issue in enumerate(validation_result.critical_issues[:3], 1):
                print(f"      {idx}. [{issue.category}] {issue.message[:80]}...")
                if issue.suggested_fix:
                    print(f"         Fix: {issue.suggested_fix[:80]}...")

            if len(validation_result.critical_issues) > 3:
                print(
                    f"      ... and {len(validation_result.critical_issues) - 3} more issues"
                )
            print("   ⚠️  Proceeding to DuckDB validation (strict judge will decide)")
        else:
            print("   ✓ No critical issues detected by LLM")

    def close(self):
        """Cleanup resources"""
        logger.info("Closing orchestrator resources")
        self.engine.close()
        self.audit.close()
        logger.info("Orchestrator resources closed successfully")


# ==========================================
# CLI ENTRY POINT
# ==========================================

if __name__ == "__main__":
    import argparse
    import sys

    # Fix Unicode encoding on Windows console
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore
            sys.stderr.reconfigure(encoding="utf-8")  # type: ignore
        except AttributeError:
            # Python < 3.7
            import codecs

            sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
            sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

    parser = argparse.ArgumentParser(
        description="Enterprise Compliance Control Orchestrator"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="data/input",
        help="Input directory containing project folders (default: data/input)",
    )
    parser.add_argument(
        "--mock-ai",
        action="store_true",
        help="Use mock AI translator (no API calls required)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/audit.db",
        help="SQLite database path (default: data/audit.db)",
    )

    args = parser.parse_args()

    # Initialize logging
    from src.utils.logging_config import setup_logging

    setup_logging()
    logger.info("Starting Enterprise Compliance Control Orchestrator")
    logger.info(f"Arguments: input={args.input}, mock_ai={args.mock_ai}, db={args.db}")

    try:
        orchestrator = BatchOrchestrator(
            use_mock_ai=args.mock_ai,
            db_path=args.db,
        )

        summary = orchestrator.process_all_projects(args.input)

        orchestrator.close()

        # Exit code based on results
        if summary["fail_count"] > 0 or summary["error_count"] > 0:
            logger.warning("Orchestrator completed with failures or errors")
            exit(1)
        else:
            logger.info("Orchestrator completed successfully")
            exit(0)

    except Exception as e:
        logger.critical(
            f"Fatal error in orchestrator: {type(e).__name__}: {str(e)}", exc_info=True
        )
        print(f"\n💥 FATAL ERROR: {type(e).__name__}: {str(e)}")
        exit(2)
