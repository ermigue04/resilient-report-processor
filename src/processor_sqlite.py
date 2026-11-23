"""Procesador resiliente de reportes sobre SQLite (sin dependencias externas)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

try:
    from classifier import clasificar_reporte
except ImportError:
    from src.classifier import clasificar_reporte

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "sql" / "schema_sqlite.sql"
SAMPLE_DATA_PATH = BASE_DIR / "data" / "sample_events.json"
DEFAULT_DB_PATH = BASE_DIR / "data" / "reports.db"

MAX_INTENTOS = 3
DEFAULT_BATCH_SIZE = 50


@dataclass(frozen=True)
class ResultadoProcesamiento:
    """Estructura uniforme de retorno para el procesamiento de cada evento."""

    event_id: str
    estado_final: str
    ok: bool
    codigo_error: str | None = None
    mensaje: str = ""
    datos: dict[str, Any] | None = None


def get_db_path() -> Path:
    """Obtiene la ruta del archivo SQLite desde REPORT_DB_PATH o usa data/reports.db."""
    return Path(os.environ.get("REPORT_DB_PATH", str(DEFAULT_DB_PATH)))


def init_db(conn: sqlite3.Connection) -> None:
    """Aplica sql/schema_sqlite.sql para crear tablas e índices."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)


def ingest_events(conn: sqlite3.Connection, events_path: Path) -> tuple[int, int]:
    """Ingiere eventos desde JSON aplicando idempotencia por external_event_id."""
    events = json.loads(events_path.read_text(encoding="utf-8"))
    inserted = 0
    duplicates = 0

    with conn:
        for event in events:
            external_id = event["external_event_id"]
            raw_payload = json.dumps(event, ensure_ascii=False)
            cur = conn.execute(
                """
                INSERT INTO reportes_ingesta (external_event_id, payload_raw)
                VALUES (?, ?)
                ON CONFLICT (external_event_id) DO NOTHING
                RETURNING id;
                """,
                (external_id, raw_payload),
            )
            if cur.fetchone() is not None:
                inserted += 1
            else:
                duplicates += 1

    return inserted, duplicates


def _marcar_estado(
    conn: sqlite3.Connection,
    ingesta_id: int,
    estado: str,
    error_msg: str | None = None,
) -> None:
    """Actualiza el estado, incrementa intentos y refresca actualizado_en."""
    conn.execute(
        """
        UPDATE reportes_ingesta
        SET estado = ?,
            intentos = intentos + 1,
            ultimo_error = ?,
            actualizado_en = datetime('now')
        WHERE id = ?;
        """,
        (estado, error_msg, ingesta_id),
    )


def _guardar_reporte_procesado(
    conn: sqlite3.Connection,
    ingesta_id: int,
    categoria: str,
    prioridad: int,
    resumen: str,
) -> None:
    """Guarda el reporte procesado de forma idempotente por ingesta_id."""
    conn.execute(
        """
        INSERT INTO reportes_procesados (ingesta_id, categoria, prioridad, resumen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (ingesta_id) DO UPDATE SET
            categoria = excluded.categoria,
            prioridad = excluded.prioridad,
            resumen = excluded.resumen,
            procesado_en = datetime('now');
        """,
        (ingesta_id, categoria, prioridad, resumen),
    )


def _procesar_registro(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> ResultadoProcesamiento:
    """Procesa un único registro de ingesta y devuelve un ResultadoProcesamiento."""
    ingesta_id = row["id"]
    event_id = row["external_event_id"]

    try:
        event = json.loads(row["payload_raw"])
        raw_inner = event.get("payload")

        if isinstance(raw_inner, str):
            payload = json.loads(raw_inner)
        elif isinstance(raw_inner, dict):
            payload = raw_inner
        else:
            raise ValueError("El campo 'payload' no es un objeto JSON válido.")

        clasificacion = clasificar_reporte(payload)
        if not clasificacion.es_valido:
            _marcar_estado(conn, ingesta_id, "revision", clasificacion.motivo)
            return ResultadoProcesamiento(
                event_id=event_id,
                estado_final="revision",
                ok=False,
                codigo_error="CLASIFICACION_INVALIDA_O_AMBIGUA",
                mensaje=clasificacion.motivo,
                datos=None,
            )

        assert clasificacion.categoria is not None
        assert clasificacion.prioridad is not None
        assert clasificacion.resumen is not None

        _guardar_reporte_procesado(
            conn,
            ingesta_id,
            clasificacion.categoria,
            clasificacion.prioridad,
            clasificacion.resumen,
        )
        _marcar_estado(conn, ingesta_id, "procesado")

        return ResultadoProcesamiento(
            event_id=event_id,
            estado_final="procesado",
            ok=True,
            codigo_error=None,
            mensaje=clasificacion.motivo,
            datos={
                "categoria": clasificacion.categoria,
                "prioridad": clasificacion.prioridad,
                "resumen": clasificacion.resumen,
            },
        )

    except json.JSONDecodeError as exc:
        mensaje_error = f"JSON corrupto: {exc}"
        _marcar_estado(conn, ingesta_id, "fallido", mensaje_error)
        return ResultadoProcesamiento(
            event_id=event_id,
            estado_final="fallido",
            ok=False,
            codigo_error="JSON_CORRUPTO",
            mensaje=mensaje_error,
            datos=None,
        )

    except ValueError as exc:
        mensaje_error = str(exc)
        _marcar_estado(conn, ingesta_id, "revision", mensaje_error)
        return ResultadoProcesamiento(
            event_id=event_id,
            estado_final="revision",
            ok=False,
            codigo_error="PAYLOAD_INVALIDO",
            mensaje=mensaje_error,
            datos=None,
        )


def process_pending(
    conn: sqlite3.Connection, batch_size: int = DEFAULT_BATCH_SIZE
) -> list[ResultadoProcesamiento]:
    """Procesa hasta batch_size registros en estado pendiente o fallido con intentos < 3."""
    rows = conn.execute(
        """
        SELECT id, external_event_id, payload_raw
        FROM reportes_ingesta
        WHERE estado IN ('pendiente', 'fallido')
          AND intentos < ?
        ORDER BY creado_en ASC
        LIMIT ?;
        """,
        (MAX_INTENTOS, batch_size),
    ).fetchall()

    resultados: list[ResultadoProcesamiento] = []
    with conn:
        for row in rows:
            resultados.append(_procesar_registro(conn, row))

    return resultados


def run_batch(batch_size: int = DEFAULT_BATCH_SIZE) -> dict[str, Any]:
    """Ejecuta ingesta y procesamiento por lotes y devuelve el run_summary."""
    inicio = time.perf_counter()
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        init_db(conn)
        ingest_events(conn, SAMPLE_DATA_PATH)
        resultados = process_pending(conn, batch_size=batch_size)

    duracion_ms = round((time.perf_counter() - inicio) * 1000, 2)
    return {
        "batch_size": batch_size,
        "leidos": len(resultados),
        "procesados": sum(1 for r in resultados if r.estado_final == "procesado"),
        "revision": sum(1 for r in resultados if r.estado_final == "revision"),
        "fallidos": sum(1 for r in resultados if r.estado_final == "fallido"),
        "duracion_ms": duracion_ms,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orquestador por lotes de resilient-report-processor (SQLite)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Cantidad máxima de registros por corrida (default: {DEFAULT_BATCH_SIZE}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        run_summary = run_batch(batch_size=args.batch_size)
    except (sqlite3.Error, OSError) as exc:
        error_payload = {
            "error_fatal": True,
            "codigo": "ERROR_INFRAESTRUCTURA_BD",
            "mensaje": str(exc),
        }
        print(json.dumps(error_payload, ensure_ascii=False), file=sys.stderr)
        return 1

    print(json.dumps(run_summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
