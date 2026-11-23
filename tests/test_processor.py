"""Tests automáticos de contratos, idempotencia y resiliencia (Fase 2)."""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "src"))

from classifier import validar_salida_clasificacion  # noqa: E402
from processor_sqlite import (  # noqa: E402
    _guardar_reporte_procesado,
    ingest_events,
    init_db,
    process_pending,
)


class TestResilientReportProcessor(unittest.TestCase):
    """Verifica las garantías principales del procesador sin depender de APIs externas."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_reports.db"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp_dir.cleanup()

    def test_validacion_estricta_rechaza_booleanos_y_valores_invalidos(self) -> None:
        # En Python isinstance(True, int) es True: debe rechazarse explícitamente.
        res_bool = validar_salida_clasificacion(
            {"categoria": "infraestructura", "prioridad": True, "resumen": "Fuga"}
        )
        self.assertFalse(res_bool.es_valido)

        res_rango = validar_salida_clasificacion(
            {"categoria": "infraestructura", "prioridad": 6, "resumen": "Fuga"}
        )
        self.assertFalse(res_rango.es_valido)

        res_cat = validar_salida_clasificacion(
            {"categoria": "inventada", "prioridad": 3, "resumen": "Fuga"}
        )
        self.assertFalse(res_cat.es_valido)

        res_ok = validar_salida_clasificacion(
            {"categoria": "infraestructura", "prioridad": 4, "resumen": "Fuga de agua"}
        )
        self.assertTrue(res_ok.es_valido)
        self.assertEqual(res_ok.prioridad, 4)

    def test_ingesta_idempotente_ignora_duplicados(self) -> None:
        events_file = Path(self.tmp_dir.name) / "events.json"
        evento = {
            "external_event_id": "evt_dup_1",
            "payload": {
                "titulo": "Fuga",
                "descripcion": "Tubería rota",
                "ubicacion": "Calle 1",
                "severidad_sugerida": "alta",
            },
        }
        events_file.write_text(json.dumps([evento, evento]), encoding="utf-8")

        insertados, duplicados = ingest_events(self.conn, events_file)
        self.assertEqual(insertados, 1)
        self.assertEqual(duplicados, 1)

        total = self.conn.execute("SELECT COUNT(*) FROM reportes_ingesta").fetchone()[0]
        self.assertEqual(total, 1)

    def test_tope_de_reintentos_detiene_fallidos_en_3(self) -> None:
        events_file = Path(self.tmp_dir.name) / "corrupt.json"
        evento_corrupto = {
            "external_event_id": "evt_corrupt_1",
            "payload": "{json_roto:",
        }
        events_file.write_text(json.dumps([evento_corrupto]), encoding="utf-8")
        ingest_events(self.conn, events_file)

        for esperado in range(1, 4):
            res = process_pending(self.conn)
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0].estado_final, "fallido")
            intentos = self.conn.execute(
                "SELECT intentos FROM reportes_ingesta WHERE external_event_id = 'evt_corrupt_1'"
            ).fetchone()[0]
            self.assertEqual(intentos, esperado)

        # En el 4to intento ya no debe seleccionarse porque intentos == 3.
        self.assertEqual(process_pending(self.conn), [])

    def test_caso_ambiguo_va_a_revision_sin_guardar_en_procesados(self) -> None:
        events_file = Path(self.tmp_dir.name) / "ambiguo.json"
        evento_ambiguo = {
            "external_event_id": "evt_amb_1",
            "payload": {
                "titulo": "Problema raro",
                "descripcion": "Hace ruido",
                "ubicacion": "",
                "severidad_sugerida": "desconocida",
            },
        }
        events_file.write_text(json.dumps([evento_ambiguo]), encoding="utf-8")
        ingest_events(self.conn, events_file)

        res = process_pending(self.conn)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].estado_final, "revision")

        procesados = self.conn.execute("SELECT COUNT(*) FROM reportes_procesados").fetchone()[0]
        self.assertEqual(procesados, 0)

    def test_guardar_reporte_procesado_es_idempotente_por_ingesta_id(self) -> None:
        events_file = Path(self.tmp_dir.name) / "valid.json"
        evento = {
            "external_event_id": "evt_ok_1",
            "payload": {
                "titulo": "Fuga",
                "descripcion": "Agua en calle",
                "ubicacion": "Av. 1",
                "severidad_sugerida": "media",
            },
        }
        events_file.write_text(json.dumps([evento]), encoding="utf-8")
        ingest_events(self.conn, events_file)
        ingesta_id = self.conn.execute("SELECT id FROM reportes_ingesta").fetchone()[0]

        with self.conn:
            _guardar_reporte_procesado(self.conn, ingesta_id, "infraestructura", 3, "Resumen 1")
            _guardar_reporte_procesado(self.conn, ingesta_id, "infraestructura", 4, "Resumen 2")

        rows = self.conn.execute("SELECT * FROM reportes_procesados").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["prioridad"], 4)
        self.assertEqual(rows[0]["resumen"], "Resumen 2")


if __name__ == "__main__":
    unittest.main()
