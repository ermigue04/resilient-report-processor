"""Procesador resiliente de reportes sobre PostgreSQL."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

try:
    from classifier import clasificar_reporte
except ImportError:
    from src.classifier import clasificar_reporte

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"
SAMPLE_DATA_PATH = BASE_DIR / "data" / "sample_events.json"

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/reports"
MAX_INTENTOS = 3


@dataclass(frozen=True)
class ResultadoProcesamiento:
    """Estructura uniforme de retorno para el procesamiento de cada evento."""

    event_id: str
    estado_final: str
    ok: bool
    codigo_error: str | None = None
    mensaje: str = ""
    datos: dict[str, Any] | None = None


def get_database_url() -> str:
    """Obtiene la cadena de conexión desde DATABASE_URL o usa el valor local por defecto."""
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def init_db(conn: psycopg.Connection[Any]) -> None:
    """Ejecuta sql/schema.sql para crear tablas e índices si no existen."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def ingest_events(conn: psycopg.Connection[Any], events_path: Path) -> tuple[int, int]:
    """Ingiere eventos desde JSON aplicando idempotencia por external_event_id."""
    events = json.loads(events_path.read_text(encoding="utf-8"))
    inserted = 0
    duplicates = 0

    with conn.cursor() as cur:
        for event in events:
            external_id = event["external_event_id"]
            raw_payload = json.dumps(event, ensure_ascii=False)
            cur.execute(
                """
                INSERT INTO reportes_ingesta (external_event_id, payload_raw)
                VALUES (%s, %s)
                ON CONFLICT (external_event_id) DO NOTHING
                RETURNING id;
                """,
                (external_id, raw_payload),
            )
            if cur.fetchone() is not None:
                inserted += 1
            else:
                duplicates += 1

    conn.commit()
    return inserted, duplicates


def _marcar_estado(
    cur: psycopg.Cursor[Any],
    ingesta_id: int,
    estado: str,
    error_msg: str | None = None,
) -> None:
    """Actualiza el estado, incrementa intentos y refresca actualizado_en."""
    cur.execute(
        """
        UPDATE reportes_ingesta
        SET estado = %s,
            intentos = intentos + 1,
            ultimo_error = %s,
            actualizado_en = NOW()
        WHERE id = %s;
        """,
        (estado, error_msg, ingesta_id),
    )


def _guardar_reporte_procesado(
    cur: psycopg.Cursor[Any],
    ingesta_id: int,
    categoria: str,
    prioridad: int,
    resumen: str,
) -> None:
    """Guarda el reporte procesado de forma idempotente por ingesta_id."""
    cur.execute(
        """
        INSERT INTO reportes_procesados (ingesta_id, categoria, prioridad, resumen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ingesta_id) DO UPDATE SET
            categoria = EXCLUDED.categoria,
            prioridad = EXCLUDED.prioridad,
            resumen = EXCLUDED.resumen,
            procesado_en = NOW();
        """,
        (ingesta_id, categoria, prioridad, resumen),
    )


def _procesar_registro(
    cur: psycopg.Cursor[Any], row: dict[str, Any]
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
            _marcar_estado(cur, ingesta_id, "revision", clasificacion.motivo)
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
            cur,
            ingesta_id,
            clasificacion.categoria,
            clasificacion.prioridad,
            clasificacion.resumen,
        )
        _marcar_estado(cur, ingesta_id, "procesado")

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
        _marcar_estado(cur, ingesta_id, "fallido", mensaje_error)
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
        _marcar_estado(cur, ingesta_id, "revision", mensaje_error)
        return ResultadoProcesamiento(
            event_id=event_id,
            estado_final="revision",
            ok=False,
            codigo_error="PAYLOAD_INVALIDO",
            mensaje=mensaje_error,
            datos=None,
        )


def process_pending(conn: psycopg.Connection[Any]) -> list[ResultadoProcesamiento]:
    """Procesa registros en estado pendiente o fallido con intentos < 3."""
    resultados: list[ResultadoProcesamiento] = []

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, external_event_id, payload_raw
            FROM reportes_ingesta
            WHERE estado IN ('pendiente', 'fallido')
              AND intentos < %s
            ORDER BY creado_en ASC
            FOR UPDATE SKIP LOCKED;
            """,
            (MAX_INTENTOS,),
        )
        rows = cur.fetchall()

        for row in rows:
            resultados.append(_procesar_registro(cur, row))

    conn.commit()
    return resultados


def main() -> list[ResultadoProcesamiento]:
    db_url = get_database_url()
    with psycopg.connect(db_url) as conn:
        init_db(conn)
        ingest_events(conn, SAMPLE_DATA_PATH)
        return process_pending(conn)


if __name__ == "__main__":
    salida = [asdict(r) for r in main()]
    print(json.dumps(salida, ensure_ascii=False, indent=2))
