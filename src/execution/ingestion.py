"""
Evidence Ingestion Module
Converts Excel to Parquet with SHA-256 hashing and source metadata
"""

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.utils.logging_config import get_logger

# Get logger for this module
logger = get_logger(__name__)


class EvidenceIngestion:
    """
    Converts Excel files to Parquet and generates cryptographic hashes.
    Handles multi-sheet workbooks and captures source metadata.
    """

    def __init__(self, storage_dir: str = "data/parquet"):
        logger.info(f"Initializing EvidenceIngestion with storage_dir={storage_dir}")
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Storage directory ready: {self.storage_dir.absolute()}")

    def ingest_excel_to_parquet(
        self,
        excel_path: str,
        dataset_prefix: str,
        source_system: str = "UNKNOWN",
        extraction_timestamp: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Loads Excel, sanitizes columns, saves to Parquet, returns manifest.

        Args:
            excel_path: Path to Excel file
            dataset_prefix: Prefix for output files (e.g., "trade_log")
            source_system: Name of source system (e.g., "SAP_FI", "Bloomberg")
            extraction_timestamp: When data was extracted from source

        Returns:
            List of manifests with parquet_path, sha256_hash, source metadata
        """
        logger.info(f"Starting ingestion of {excel_path}")
        logger.debug(
            f"Dataset prefix: {dataset_prefix}, source system: {source_system}"
        )
        path = Path(excel_path)

        if not path.exists():
            logger.error(f"Excel file not found: {excel_path}")
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        # Load all sheets
        logger.debug(f"Loading Excel file: {path.name}")
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        logger.info(f"Loaded {len(sheets)} sheet(s) from {path.name}")
        manifests = []

        for sheet_name, df in sheets.items():
            logger.debug(
                f"Processing sheet: {sheet_name} ({len(df)} rows, {len(df.columns)} columns)"
            )
            # 1. Sanitize column names
            df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]
            logger.debug(
                f"Sanitized columns: {list(df.columns)[:10]}..."
            )  # Show first 10

            # 2. Type casting to prevent DuckDB schema inference errors
            df = self._cast_types(df)

            # 3. Save to Parquet
            sanitized_sheet = sheet_name.replace(" ", "_").lower()
            out_path = self.storage_dir / f"{dataset_prefix}_{sanitized_sheet}.parquet"
            logger.debug(f"Saving to Parquet: {out_path.name}")
            df.to_parquet(out_path, index=False, engine="pyarrow")

            # 4. Generate SHA-256 hash of physical file
            logger.debug("Calculating SHA-256 hash")
            file_hash = self._hash_file(out_path)
            logger.debug(f"Hash: {file_hash[:16]}...")

            # 5. Calculate schema version hash
            schema_version = self._calculate_schema_version(df)
            logger.debug(f"Schema version: {schema_version}")

            manifests.append(
                {
                    "dataset_alias": f"{dataset_prefix}_{sanitized_sheet}",
                    "parquet_path": str(out_path),
                    "sha256_hash": file_hash,
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "source_system": source_system,
                    "extraction_timestamp": extraction_timestamp.isoformat()
                    if extraction_timestamp
                    else None,
                    "schema_version": schema_version,
                    "ingested_at": datetime.now().isoformat(),
                    "columns": list(df.columns),
                }
            )
            logger.info(
                f"Sheet {sheet_name} ingested: {len(df)} rows, "
                f"hash={file_hash[:12]}..., schema_version={schema_version}"
            )

        logger.info(f"Ingestion complete: {len(manifests)} manifest(s) created")
        return manifests

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casts ambiguous types and cleans dirty Excel formatting
        to ensure pristine DuckDB execution.

        This is Layer 2: The Data Quality Gate (Defense in Depth)
        - Layer 1: Pre-processing (universal fixes)
        - Layer 2: Quality Gate (type coercion, currency cleaning)
        - Layer 3: SQL Validation (business logic)
        """
        logger.debug("Casting data types for DuckDB compatibility")
        for col in df.columns:
            # 1. Cast ID/Code columns to string
            if any(keyword in col.lower() for keyword in ["id", "code", "number"]):
                df[col] = df[col].astype(str)
                continue

            # 2. Clean Currency & Numeric columns (The DeepSeek Fix)
            # Handle formats like "$12,345.67" or "€1.234,56" or "1,234.56"
            # If the column is an object (string) but looks like currency
            if df[col].dtype == "object":
                # Check if column contains currency-like patterns
                sample = df[col].dropna().astype(str).head(100)
                if (
                    len(sample) > 0
                    and sample.str.match(r"^[\s$€£¥]*[\d,\.]+$", na=False).any()
                ):
                    logger.debug(
                        f"Detected currency/numeric formatting in column: {col}"
                    )
                    # Strip currency symbols ($, £, €, ¥), spaces, and commas
                    df[col] = (
                        df[col].astype(str).str.replace(r"[^\d.-]", "", regex=True)
                    )
                    # Convert to numeric, coerce errors to NaN
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    logger.debug(f"Cleaned {col}: converted to numeric")

        # 3. Standardize Datetimes (Leave as native datetime for Parquet)
        # Force valid dates, but LEAVE THEM AS NATIVE DATETIME OBJECTS
        # so PyArrow writes them as Parquet Timestamps, not Varchar strings.
        for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        # Catch hidden string dates in 'date' columns
        date_cols = [
            c for c in df.columns if "date" in c.lower() and df[c].dtype == "object"
        ]
        for col in date_cols:
            try:
                # Infer datetime formats, coerce errors to NaT
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.debug(f"Converted date column: {col}")
            except Exception:
                pass  # Leave as string if it completely fails to parse

        return df

    @staticmethod
    def _hash_file(filepath: Path) -> str:
        """Generates SHA-256 hash of file contents"""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def _calculate_schema_version(df: pd.DataFrame) -> str:
        """
        Generates a version hash based on column names and types.
        Changes to schema will produce different version.
        """
        schema_str = ",".join(
            [f"{col}:{str(dtype)}" for col, dtype in zip(df.columns, df.dtypes)]
        )
        return hashlib.md5(schema_str.encode()).hexdigest()[:16]

    def get_column_headers(self, excel_path: str) -> Dict[str, List[str]]:
        """
        Extracts column headers from Excel without loading full data.
        Used for AI schema pruning.
        """
        logger.debug(f"Extracting column headers from {excel_path}")
        path = Path(excel_path)
        sheets = pd.read_excel(path, sheet_name=None, nrows=0, engine="openpyxl")

        headers = {}
        for sheet_name, df in sheets.items():
            sanitized_cols = [
                str(c).strip().replace(" ", "_").lower() for c in df.columns
            ]
            headers[sheet_name] = sanitized_cols

        return headers
