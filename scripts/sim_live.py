"""Simulador de prueba en vivo: inyecta 1 evento nuevo + 1 duplicado y procesa sin borrar la BD."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import sqlite3
import sys
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "src"))

from processor_sqlite import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    get_db_path,
    init_db,
    process_pending,
)


def main() -> int:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    new_event_id = f"evt_live_{ts}"

    eventos_simulados = [
        {
            "case": "live_nuevo",
            "external_event_id": new_event_id,
            "source": "sim_live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "titulo": "Falla en poste de alumbrado",
                "descripcion": "Luminaria apagada y cables sueltos frente al mercado municipal.",
                "ubicacion": "Calle 10 y Av. Bolívar",
                "severidad_sugerida": "alta",
            },
        },
        {
            "case": "live_duplicado",
            "external_event_id": new_event_id,
            "source": "sim_live",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": {
                "titulo": "Falla en poste de alumbrado",
                "descripcion": "Luminaria apagada y cables sueltos frente al mercado municipal.",
                "ubicacion": "Calle 10 y Av. Bolívar",
                "severidad_sugerida": "alta",
            },
        },
    ]

    inicio = time.perf_counter()
    inserted = 0
    duplicates = 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        init_db(conn)

        with conn:
            for ev in eventos_simulados:
                cur = conn.execute(
                    """
                    INSERT INTO reportes_ingesta (external_event_id, payload_raw)
                    VALUES (?, ?)
                    ON CONFLICT (external_event_id) DO NOTHING
                    RETURNING id;
                    """,
                    (ev["external_event_id"], json.dumps(ev, ensure_ascii=False)),
                )
                if cur.fetchone() is not None:
                    inserted += 1
                else:
                    duplicates += 1

        resultados = process_pending(conn, batch_size=DEFAULT_BATCH_SIZE)

    salida = {
        "live_event_id": new_event_id,
        "ingesta": {"insertados": inserted, "duplicados_ignorados": duplicates},
        "procesamiento": [asdict(r) for r in resultados],
        "duracion_ms": round((time.perf_counter() - inicio) * 1000, 2),
    }
    print(json.dumps(salida, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
