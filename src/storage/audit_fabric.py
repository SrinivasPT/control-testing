"""
Audit Fabric Module - SQLite Implementation
Immutable storage for control DSLs, execution history, and evidence manifests
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path


class AuditFabric:
    """
    SQLite-based audit ledger for control governance and execution history.
    Provides immutable storage with cryptographic hash verification.
    """

    def __init__(self, db_path: str = "data/audit.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        self._init_schema()

    def _init_schema(self):
        """Creates database schema if not exists"""
        cursor = self.conn.cursor()

        # Controls table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS controls (
                control_id TEXT PRIMARY KEY,
                dsl_json TEXT NOT NULL,
                version TEXT NOT NULL,
                owner_role TEXT,
                approved_by TEXT,
                approved_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(control_id, version)
            )
        """)

        # Evidence manifests table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS evidence_manifests (
                manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_alias TEXT NOT NULL,
                parquet_path TEXT NOT NULL,
                sha256_hash TEXT NOT NULL,
                row_count INTEGER,
                column_count INTEGER,
                source_system TEXT,
                extraction_timestamp TEXT,
                extraction_query_hash TEXT,
                schema_version TEXT,
                ingested_at TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Executions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                control_id TEXT NOT NULL,
                verdict TEXT NOT NULL CHECK(verdict IN ('PASS', 'FAIL', 'ERROR')),
                exception_count INTEGER,
                total_population INTEGER,
                exception_rate_percent REAL,
                compiled_sql TEXT NOT NULL,
                evidence_hashes TEXT NOT NULL,
                exceptions_sample TEXT,
                error_message TEXT,
                executed_at TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (control_id) REFERENCES controls(control_id)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_control 
            ON executions(control_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_verdict 
            ON executions(verdict)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_date 
            ON executions(executed_at)
        """)

        self.conn.commit()

    def save_control(self, dsl: Dict[str, Any], approved_by: str) -> None:
        """
        Persists approved DSL to immutable store.

        Args:
            dsl: The EnterpriseControlDSL as dictionary
            approved_by: Username of approver
        """
        cursor = self.conn.cursor()

        control_id = dsl["governance"]["control_id"]
        version = dsl["governance"]["version"]
        owner_role = dsl["governance"]["owner_role"]

        cursor.execute(
            """
            INSERT OR REPLACE INTO controls 
            (control_id, dsl_json, version, owner_role, approved_by, approved_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                control_id,
                json.dumps(dsl, indent=2),
                version,
                owner_role,
                approved_by,
                datetime.now().isoformat(),
            ),
        )

        self.conn.commit()

    def get_control(self, control_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves approved DSL by control_id"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT dsl_json FROM controls WHERE control_id = ?
        """,
            (control_id,),
        )

        row = cursor.fetchone()
        if row:
            return json.loads(row["dsl_json"])
        return None

    def save_evidence_manifest(self, manifest: Dict[str, Any]) -> int:
        """
        Saves evidence manifest with source metadata.

        Returns:
            manifest_id
        """
        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT INTO evidence_manifests 
            (dataset_alias, parquet_path, sha256_hash, row_count, column_count,
             source_system, extraction_timestamp, schema_version, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                manifest["dataset_alias"],
                manifest["parquet_path"],
                manifest["sha256_hash"],
                manifest["row_count"],
                manifest["column_count"],
                manifest.get("source_system"),
                manifest.get("extraction_timestamp"),
                manifest.get("schema_version"),
                manifest["ingested_at"],
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def save_execution(self, report: Dict[str, Any]) -> None:
        """
        Logs execution result for audit trail.

        Args:
            report: Execution report from ExecutionEngine
        """
        import uuid

        cursor = self.conn.cursor()

        execution_id = str(uuid.uuid4())

        cursor.execute(
            """
            INSERT INTO executions 
            (execution_id, control_id, verdict, exception_count, total_population,
             exception_rate_percent, compiled_sql, evidence_hashes, exceptions_sample,
             error_message, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                execution_id,
                report["control_id"],
                report["verdict"],
                report.get("exception_count"),
                report.get("total_population"),
                report.get("exception_rate_percent"),
                report["execution_query"],
                json.dumps(report.get("evidence_hashes", {})),
                json.dumps(report.get("exceptions_sample", [])),
                report.get("error_message"),
                report["executed_at"],
            ),
        )

        self.conn.commit()

    def get_execution_history(
        self, control_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Retrieves execution history for a control.

        Returns:
            List of execution records
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT 
                execution_id,
                control_id,
                verdict,
                exception_count,
                total_population,
                exception_rate_percent,
                executed_at
            FROM executions
            WHERE control_id = ?
            ORDER BY executed_at DESC
            LIMIT ?
        """,
            (control_id, limit),
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_audit_evidence_lineage(
        self, execution_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Audit view: Verifies evidence integrity by comparing hashes.

        Returns:
            List of evidence lineage records with integrity status
        """
        cursor = self.conn.cursor()

        if execution_id:
            cursor.execute(
                """
                SELECT 
                    e.execution_id,
                    e.control_id,
                    e.verdict,
                    e.executed_at,
                    m.dataset_alias,
                    m.sha256_hash as stored_hash,
                    json_extract(e.evidence_hashes, '$.' || m.dataset_alias) as execution_hash
                FROM executions e
                JOIN evidence_manifests m ON json_extract(e.evidence_hashes, '$.' || m.dataset_alias) IS NOT NULL
                WHERE e.execution_id = ?
            """,
                (execution_id,),
            )
        else:
            cursor.execute("""
                SELECT 
                    e.execution_id,
                    e.control_id,
                    e.verdict,
                    e.executed_at,
                    m.dataset_alias,
                    m.sha256_hash as stored_hash,
                    json_extract(e.evidence_hashes, '$.' || m.dataset_alias) as execution_hash
                FROM executions e
                JOIN evidence_manifests m ON json_extract(e.evidence_hashes, '$.' || m.dataset_alias) IS NOT NULL
                LIMIT 100
            """)

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            # Determine integrity status
            row_dict["integrity_status"] = (
                "VALID"
                if row_dict["stored_hash"] == row_dict["execution_hash"]
                else "TAMPERED"
            )
            results.append(row_dict)

        return results

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """
        Returns summary statistics for monitoring dashboard.
        """
        cursor = self.conn.cursor()

        # Total controls
        cursor.execute("SELECT COUNT(*) FROM controls")
        total_controls = cursor.fetchone()[0]

        # Recent executions (last 30 days)
        cursor.execute("""
            SELECT 
                verdict,
                COUNT(*) as count
            FROM executions
            WHERE datetime(executed_at) >= datetime('now', '-30 days')
            GROUP BY verdict
        """)
        verdict_counts = {row["verdict"]: row["count"] for row in cursor.fetchall()}

        # Average exception rate
        cursor.execute("""
            SELECT 
                AVG(exception_rate_percent) as avg_exception_rate
            FROM executions
            WHERE verdict IN ('PASS', 'FAIL')
                AND datetime(executed_at) >= datetime('now', '-30 days')
        """)
        avg_exception_rate = cursor.fetchone()["avg_exception_rate"] or 0

        return {
            "total_controls": total_controls,
            "last_30_days": {
                "pass_count": verdict_counts.get("PASS", 0),
                "fail_count": verdict_counts.get("FAIL", 0),
                "error_count": verdict_counts.get("ERROR", 0),
                "avg_exception_rate": round(avg_exception_rate, 2),
            },
        }

    def close(self):
        """Close database connection"""
        self.conn.close()
