-- Esquema inicial para resilient-report-processor (PostgreSQL)

CREATE TABLE IF NOT EXISTS reportes_ingesta (
    id BIGSERIAL PRIMARY KEY,
    external_event_id VARCHAR(255) NOT NULL UNIQUE,
    payload_raw TEXT NOT NULL,
    estado VARCHAR(20) NOT NULL DEFAULT 'pendiente'
        CHECK (estado IN ('pendiente', 'procesado', 'revision', 'fallido')),
    intentos INTEGER NOT NULL DEFAULT 0
        CHECK (intentos >= 0),
    ultimo_error TEXT,
    creado_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reportes_procesados (
    id BIGSERIAL PRIMARY KEY,
    ingesta_id BIGINT NOT NULL UNIQUE
        REFERENCES reportes_ingesta(id) ON DELETE CASCADE,
    categoria VARCHAR(100) NOT NULL,
    prioridad INTEGER NOT NULL
        CHECK (prioridad BETWEEN 1 AND 5),
    resumen TEXT NOT NULL,
    procesado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índice para consultar o hacer polling de reportes por estado (y ordenarlos por antigüedad)
CREATE INDEX IF NOT EXISTS idx_reportes_ingesta_estado_creado
    ON reportes_ingesta (estado, creado_en);

-- Índice explícito por external_event_id (aunque UNIQUE ya crea uno implícito en PostgreSQL,
-- se documenta aquí la intención de búsqueda rápida por clave de idempotencia)
CREATE INDEX IF NOT EXISTS idx_reportes_ingesta_external_event_id
    ON reportes_ingesta (external_event_id);
