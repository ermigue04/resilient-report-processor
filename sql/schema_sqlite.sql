-- Esquema compatible con SQLite para iterar localmente sin servidor.
-- Equivalente directo de sql/schema.sql (PostgreSQL).

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS reportes_ingesta (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_event_id TEXT NOT NULL UNIQUE,
    payload_raw TEXT NOT NULL,
    estado TEXT NOT NULL DEFAULT 'pendiente'
        CHECK (estado IN ('pendiente', 'procesado', 'revision', 'fallido')),
    intentos INTEGER NOT NULL DEFAULT 0
        CHECK (intentos >= 0),
    ultimo_error TEXT,
    creado_en TEXT NOT NULL DEFAULT (datetime('now')),
    actualizado_en TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS reportes_procesados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ingesta_id INTEGER NOT NULL UNIQUE
        REFERENCES reportes_ingesta(id) ON DELETE CASCADE,
    categoria TEXT NOT NULL,
    prioridad INTEGER NOT NULL
        CHECK (prioridad BETWEEN 1 AND 5),
    resumen TEXT NOT NULL,
    procesado_en TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Índice para consultar reportes por estado ordenados por antigüedad
CREATE INDEX IF NOT EXISTS idx_reportes_ingesta_estado_creado
    ON reportes_ingesta (estado, creado_en);

-- Índice explícito por external_event_id (clave de idempotencia)
CREATE INDEX IF NOT EXISTS idx_reportes_ingesta_external_event_id
    ON reportes_ingesta (external_event_id);
