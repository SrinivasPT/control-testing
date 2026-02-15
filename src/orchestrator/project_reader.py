"""
Project Reader Module
Single Responsibility: Read project metadata and discover evidence files
"""

import re
from pathlib import Path
from typing import List, Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class ProjectInfo:
    """Data class for project information"""

    def __init__(
        self,
        project_name: str,
        project_path: Path,
        control_id: str,
        control_text: str,
        excel_files: List[Path],
    ):
        self.project_name = project_name
        self.project_path = project_path
        self.control_id = control_id
        self.control_text = control_text
        self.excel_files = excel_files


class ProjectReader:
    """
    Reads control information and discovers evidence files.
    Pure data reader - no business logic.
    """

    @staticmethod
    def read_project(project_path: Path) -> Optional[ProjectInfo]:
        """
        Reads project metadata and discovers evidence files.

        Args:
            project_path: Path to project directory

        Returns:
            ProjectInfo object or None if project should be skipped

        Raises:
            FileNotFoundError: If control-information.md is missing
        """
        project_name = project_path.name
        logger.debug(f"Reading project: {project_name}")

        # Step 1: Read control information
        control_information_file = project_path / "control-information.md"
        if not control_information_file.exists():
            logger.error(f"control-information.md not found in {project_name}")
            raise FileNotFoundError(
                f"control-information.md not found in {project_name}"
            )

        with open(control_information_file, "r", encoding="utf-8") as f:
            control_text = f.read()

        # Step 2: Extract control ID
        control_id = ProjectReader._extract_control_id(control_text, project_name)
        logger.info(f"Extracted control ID: {control_id} from {project_name}")

        # Step 3: Discover Excel files
        excel_files = list(project_path.glob("*.xlsx")) + list(
            project_path.glob("*.xls")
        )
        # Filter out temporary Excel files
        excel_files = [f for f in excel_files if not f.name.startswith("~$")]

        if not excel_files:
            logger.warning(f"No Excel files found in {project_name}")
            return None

        logger.info(
            f"Found {len(excel_files)} Excel files in {project_name}: {[f.name for f in excel_files]}"
        )

        return ProjectInfo(
            project_name=project_name,
            project_path=project_path,
            control_id=control_id,
            control_text=control_text,
            excel_files=excel_files,
        )

    @staticmethod
    def _extract_control_id(control_text: str, fallback_project_name: str) -> str:
        """
        Extracts control ID from markdown text.
        Looks for patterns like: "CTRL-908101" or "Control ID: XYZ-123"
        """
        # Pattern 1: "CTRL-XXXXXX"
        match = re.search(r"CTRL-[\w\-]+", control_text)
        if match:
            return match.group(0)

        # Pattern 2: "Control ID: XYZ-123"
        match = re.search(r"Control ID:\s*([A-Z0-9\-]+)", control_text, re.IGNORECASE)
        if match:
            return match.group(1)

        # Fallback: use project folder name
        logger.warning(
            f"Could not extract control ID, using fallback: {fallback_project_name}"
        )
        return fallback_project_name

    @staticmethod
    def discover_projects(input_dir: str) -> List[Path]:
        """
        Discovers all project folders in input directory.

        Args:
            input_dir: Base directory containing project folders

        Returns:
            List of project folder paths

        Raises:
            FileNotFoundError: If input directory doesn't exist
        """
        base_path = Path(input_dir)

        if not base_path.exists():
            logger.error(f"Input directory not found: {input_dir}")
            raise FileNotFoundError(f"Input directory not found: {input_dir}")

        project_folders = [f for f in base_path.iterdir() if f.is_dir()]
        logger.info(f"Discovered {len(project_folders)} project folders in {input_dir}")

        return sorted(project_folders)
