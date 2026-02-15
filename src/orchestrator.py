"""
Orchestrator Module
End-to-end batch processor for multiple control projects
Integrates all 5 layers: AI → DSL → Compiler → Execution → Audit
"""

import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from src.ai.translator import AITranslator
from src.execution.engine import ExecutionEngine
from src.execution.ingestion import EvidenceIngestion
from src.models.dsl import EnterpriseControlDSL
from src.storage.audit_fabric import AuditFabric
from src.utils.logging_config import get_logger, setup_logging

# Load environment variables
load_dotenv()

# Get logger for this module
logger = get_logger(__name__)


class BatchOrchestrator:
    """
    Orchestrates end-to-end control testing for multiple projects.

    Workflow:
    1. Scan data/input/ for project folders
    2. For each project:
       a. Read control-information.md
       b. Check if DSL exists in audit database
       c. If not, generate DSL from verification procedure (AI)
       d. Ingest Excel evidence to Parquet with SHA-256 hashing
       e. Execute deterministic testing via DuckDB
       f. Store results in audit ledger
    3. Generate summary report
    """

    def __init__(
        self,
        use_mock_ai: bool = False,
        db_path: str = "data/audit.db",
        parquet_dir: str = "data/parquet",
    ):
        """
        Initialize orchestrator with all layer dependencies.

        Args:
            use_mock_ai: If True, skip real LLM calls (for testing)
            db_path: Path to SQLite audit database
            parquet_dir: Directory for Parquet storage
        """
        logger.info("Initializing BatchOrchestrator")
        logger.debug(
            f"Configuration: use_mock_ai={use_mock_ai}, db_path={db_path}, parquet_dir={parquet_dir}"
        )

        # Layer 1: AI Translator
        if use_mock_ai:
            from src.ai.translator import MockAITranslator

            logger.info("Using MockAITranslator (no API calls)")
            self.ai = MockAITranslator()
        else:
            api_key = os.getenv("DEEPSEEK_API_KEY")
            if not api_key:
                logger.error("DEEPSEEK_API_KEY not found in environment")
                raise ValueError(
                    "DEEPSEEK_API_KEY not found. Set in .env or use use_mock_ai=True"
                )
            logger.info("Initializing AITranslator with DeepSeek API")
            self.ai = AITranslator(api_key=api_key)

        # Layer 3: Ingestion
        logger.debug("Initializing EvidenceIngestion layer")
        self.ingestion = EvidenceIngestion(storage_dir=parquet_dir)

        # Layer 4: Execution Engine
        logger.debug("Initializing ExecutionEngine layer")
        self.engine = ExecutionEngine()

        # Layer 5: Audit Fabric
        logger.debug("Initializing AuditFabric layer")
        self.audit = AuditFabric(db_path=db_path)

        logger.info("BatchOrchestrator initialization complete")

    def process_all_projects(self, input_dir: str = "data/input") -> Dict[str, Any]:
        """
        Main entry point: processes all project folders in input directory.

        Args:
            input_dir: Base directory containing project folders

        Returns:
            Summary report with overall statistics
        """
        logger.info(f"Starting batch processing from directory: {input_dir}")
        base_path = Path(input_dir)

        if not base_path.exists():
            logger.error(f"Input directory not found: {input_dir}")
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        project_folders = [f for f in base_path.iterdir() if f.is_dir()]
        logger.debug(f"Discovered {len(project_folders)} project folders")

        if not project_folders:
            logger.warning(f"No project folders found in {input_dir}")
            print(f"⚠️  No project folders found in {input_dir}")
            return {"total_projects": 0, "results": []}

        logger.info(f"Processing {len(project_folders)} projects")
        print(f"\n{'=' * 60}")
        print("🚀 ENTERPRISE CONTROL ORCHESTRATOR")
        print(f"{'=' * 60}")
        print(f"Found {len(project_folders)} project(s) to process\n")

        results = []

        for project_path in sorted(project_folders):
            project_name = project_path.name
            logger.info(f"Starting processing for project: {project_name}")
            print(f"{'─' * 60}")
            print(f"📁 Processing Project: {project_name}")
            print(f"{'─' * 60}")

            try:
                result = self._process_single_project(project_path)
                results.append(result)
                logger.info(
                    f"Project {project_name} completed with verdict: {result['verdict']}"
                )

                # Print verdict
                verdict_emoji = {
                    "PASS": "✅",
                    "FAIL": "❌",
                    "ERROR": "⚠️",
                    "SKIPPED": "⏭️",
                }
                emoji = verdict_emoji.get(result["verdict"], "❓")
                print(f"\n{emoji} VERDICT: {result['verdict']}")

                if result["verdict"] in ["PASS", "FAIL"]:
                    print(
                        f"   Exceptions: {result['exception_count']}/{result['total_population']} "
                        f"({result['exception_rate']:.2f}%)"
                    )
                elif result["verdict"] == "ERROR":
                    print(f"   Error: {result.get('error', 'Unknown')}")

            except Exception as e:
                logger.error(
                    f"Critical error processing project {project_name}: {type(e).__name__}: {str(e)}",
                    exc_info=True,
                )
                print(f"\n❌ CRITICAL ERROR: {type(e).__name__}: {str(e)}")
                results.append(
                    {
                        "project_name": project_name,
                        "verdict": "ERROR",
                        "error": str(e),
                    }
                )

        # Generate summary
        summary = self._generate_summary(results)
        self._print_summary(summary)

        return summary

    def _process_single_project(self, project_path: Path) -> Dict[str, Any]:
        """
        Processes a single project through the full pipeline.

        Returns:
            Execution result dictionary
        """
        project_name = project_path.name

        # Step 1: Read control information
        logger.debug(f"Step 1/5: Reading control information for {project_name}")
        print("\n[1/5] 📄 Reading control-information.md...")
        control_information_file = project_path / "control-information.md"

        if not control_information_file.exists():
            logger.error(f"control-information.md not found in {project_name}")
            raise FileNotFoundError(
                f"control-information.md not found in {project_name}"
            )

        with open(control_information_file, "r", encoding="utf-8") as f:
            control_text = f.read()

        # Extract Control ID from markdown (assumes first line format: # Control Testing Steps for CTRL-XXXXXX)
        control_id = self._extract_control_id(control_text, project_name)
        logger.info(f"Extracted control ID: {control_id}")
        print(f"   Control ID: {control_id}")

        # Step 2: Find Excel evidence files
        logger.debug(f"Step 2/5: Scanning for Excel files in {project_name}")
        print("\n[2/5] 📊 Scanning for Excel evidence files...")
        excel_files = [
            f for f in project_path.glob("*.xlsx") if not f.name.startswith("~$")
        ] + [f for f in project_path.glob("*.xls") if not f.name.startswith("~$")]

        if not excel_files:
            logger.warning(f"No Excel files found for project {project_name}")
            print("   ⚠️  No Excel files found - skipping project")
            return {
                "project_name": project_name,
                "control_id": control_id,
                "verdict": "SKIPPED",
                "reason": "No Excel evidence files found",
            }

        logger.info(
            f"Found {len(excel_files)} Excel files: {[f.name for f in excel_files]}"
        )
        print(
            f"   Found {len(excel_files)} file(s): {', '.join(f.name for f in excel_files)}"
        )

        # Step 3: DSL Check & Generation
        logger.debug(f"Step 3/5: Checking for existing DSL for control {control_id}")
        print("\n[3/5] 🧠 Checking for existing DSL in audit database...")
        dsl_dict = self.audit.get_control(control_id)

        if dsl_dict:
            logger.info(
                f"DSL found in database for {control_id}, version {dsl_dict['governance']['version']}"
            )
            print(
                f"   ✓ DSL found (version {dsl_dict['governance']['version']}) - reusing cached version"
            )
            dsl = EnterpriseControlDSL(**dsl_dict)
        else:
            logger.info(f"DSL not found for {control_id}, triggering AI generation")
            print("   ⚠️  DSL not found - triggering AI generation...")

            # Extract headers for schema pruning (safe - no PII exposed to AI)
            logger.debug("Extracting column headers from Excel files")
            headers = {}
            for excel in excel_files:
                try:
                    sheet_headers = self.ingestion.get_column_headers(str(excel))
                    for sheet_name, cols in sheet_headers.items():
                        dataset_alias = f"{excel.stem}_{sheet_name}".lower()
                        headers[dataset_alias] = cols
                        logger.debug(
                            f"Extracted {len(cols)} columns from {dataset_alias}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to extract headers from {excel.name}: {e}")
                    print(f"   ⚠️  Failed to extract headers from {excel.name}: {e}")

            if not headers:
                logger.error("No headers could be extracted from any Excel files")
                raise ValueError("No headers could be extracted from Excel files")

            # Generate DSL via AI (only sees headers, never row data)
            logger.info(f"Calling AI translator for control {control_id}")
            print("   🤖 Calling AI translator (model: deepseek-chat)...")
            dsl = self.ai.translate_control(control_text, headers)
            logger.info(f"AI translation completed successfully for {control_id}")

            # Override control_id if AI hallucinated
            dsl.governance.control_id = control_id

            # Save as Draft (Enterprise: would require human approval before production use)
            logger.debug(f"Saving generated DSL to audit database for {control_id}")
            self.audit.save_control(
                dsl.model_dump(), approved_by="AUTO_GENERATED_SYSTEM"
            )

            logger.info(
                f"DSL generated and saved for {control_id}, version {dsl.governance.version}"
            )
            print(f"   ✓ DSL generated and saved (version {dsl.governance.version})")

        # Step 4: Ingest Evidence (Excel → Parquet + SHA-256)
        logger.debug(f"Step 4/5: Ingesting Excel files to Parquet for {project_name}")
        print(
            "\n[4/5] 🔄 Ingesting Excel files to Parquet with cryptographic hashing..."
        )
        manifests = {}

        for excel in excel_files:
            dataset_prefix = excel.stem.lower()
            logger.debug(f"Processing evidence file: {excel.name}")
            print(f"   Processing: {excel.name}...")

            try:
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

            except Exception as e:
                logger.error(f"Failed to ingest {excel.name}: {e}", exc_info=True)
                print(f"      ❌ Failed: {e}")
                raise

        # Step 5: Execute Deterministic Control Test
        logger.debug(f"Step 5/5: Executing control test for {control_id}")
        print("\n[5/5] ⚙️  Executing control via DuckDB SQL engine...")
        report = self.engine.execute_control(dsl, manifests)
        logger.info(
            f"Execution complete for {control_id}: verdict={report['verdict']}, "
            f"exceptions={report.get('exception_count', 0)}/{report.get('total_population', 0)}"
        )

        # Save execution to audit ledger
        self.audit.save_execution(report)
        logger.debug(f"Execution report saved to audit database for {control_id}")
        print("   ✓ Execution complete - results saved to audit database")

        return {
            "project_name": project_name,
            "control_id": control_id,
            "verdict": report["verdict"],
            "exception_count": report.get("exception_count", 0),
            "total_population": report.get("total_population", 0),
            "exception_rate": report.get("exception_rate_percent", 0),
            "dsl_cached": dsl_dict is not None,
        }

    @staticmethod
    def _extract_control_id(control_text: str, fallback_project_name: str) -> str:
        """
        Extracts control ID from markdown text.
        Looks for patterns like: "CTRL-908101" or "Control ID: XYZ-123"
        """
        import re

        # Pattern 1: "# Control Testing Steps for CTRL-XXXXXX"
        match = re.search(r"CTRL-\d+", control_text)
        if match:
            return match.group(0)

        # Pattern 2: "# Control ID: XYZ-123"
        match = re.search(r"Control ID:\s*([A-Z0-9\-]+)", control_text, re.IGNORECASE)
        if match:
            return match.group(1)

        # Fallback: use project folder name
        return fallback_project_name

    def _generate_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregates execution results into summary statistics"""
        total = len(results)
        pass_count = sum(1 for r in results if r["verdict"] == "PASS")
        fail_count = sum(1 for r in results if r["verdict"] == "FAIL")
        error_count = sum(1 for r in results if r["verdict"] == "ERROR")
        skipped_count = sum(1 for r in results if r["verdict"] == "SKIPPED")

        cached_dsl_count = sum(1 for r in results if r.get("dsl_cached", False))
        generated_dsl_count = total - cached_dsl_count - error_count - skipped_count

        return {
            "total_projects": total,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "error_count": error_count,
            "skipped_count": skipped_count,
            "cached_dsl_count": cached_dsl_count,
            "generated_dsl_count": generated_dsl_count,
            "results": results,
        }

    def _print_summary(self, summary: Dict[str, Any]):
        """Prints formatted summary report"""
        print(f"\n{'=' * 60}")
        print("📊 EXECUTION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total Projects:      {summary['total_projects']}")
        print(f"  ✅ PASS:           {summary['pass_count']}")
        print(f"  ❌ FAIL:           {summary['fail_count']}")
        print(f"  ⚠️  ERROR:          {summary['error_count']}")
        print(f"  ⏭️  SKIPPED:        {summary['skipped_count']}")
        print("\nDSL Generation:")
        print(f"  🔄 Cached (DB):    {summary['cached_dsl_count']}")
        print(f"  🤖 AI Generated:   {summary['generated_dsl_count']}")
        print(f"{'=' * 60}\n")

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
