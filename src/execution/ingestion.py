"""
Evidence Ingestion Module
Converts Excel to Parquet with SHA-256 hashing and source metadata
"""

import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any


class EvidenceIngestion:
    """
    Converts Excel files to Parquet and generates cryptographic hashes.
    Handles multi-sheet workbooks and captures source metadata.
    """

    def __init__(self, storage_dir: str = "data/parquet"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

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
        path = Path(excel_path)

        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        # Load all sheets
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        manifests = []

        for sheet_name, df in sheets.items():
            # 1. Sanitize column names
            df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]

            # 2. Type casting to prevent DuckDB schema inference errors
            df = self._cast_types(df)

            # 3. Save to Parquet
            sanitized_sheet = sheet_name.replace(" ", "_").lower()
            out_path = self.storage_dir / f"{dataset_prefix}_{sanitized_sheet}.parquet"
            df.to_parquet(out_path, index=False, engine="pyarrow")

            # 4. Generate SHA-256 hash of physical file
            file_hash = self._hash_file(out_path)

            # 5. Calculate schema version hash
            schema_version = self._calculate_schema_version(df)

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

        return manifests

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casts ambiguous types to avoid DuckDB errors.
        ID columns that Pandas infers as int64 should be strings.
        """
        for col in df.columns:
            # Cast ID/Code columns to string
            if any(keyword in col.lower() for keyword in ["id", "code", "number"]):
                df[col] = df[col].astype(str)

        # Convert datetime columns to ISO format strings
        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Convert date columns
        date_cols = [c for c in df.columns if "date" in c.lower()]
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                df[col] = df[col].dt.strftime("%Y-%m-%d")
            except:
                pass

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
        path = Path(excel_path)
        sheets = pd.read_excel(path, sheet_name=None, nrows=0, engine="openpyxl")

        headers = {}
        for sheet_name, df in sheets.items():
            sanitized_cols = [
                str(c).strip().replace(" ", "_").lower() for c in df.columns
            ]
            headers[sheet_name] = sanitized_cols

        return headers
