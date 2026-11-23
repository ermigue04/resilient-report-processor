"""Capa de clasificación y extracción con IA y validación estricta."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Any
import urllib.error
import urllib.request

CATEGORIAS_PERMITIDAS = {"infraestructura", "operaciones", "acceso", "otro"}
MAPA_SEVERIDAD_FALLBACK = {
    "baja": 2,
    "media": 3,
    "alta": 4,
    "critica": 5,
}


@dataclass(frozen=True)
class ResultadoClasificacion:
    """Salida estructurada y validada de la clasificación de un reporte."""

    categoria: str | None
    prioridad: int | None
    resumen: str | None
    es_valido: bool
    motivo: str


def validar_salida_clasificacion(raw: dict[str, Any]) -> ResultadoClasificacion:
    """Valida estrictamente los tipos, rangos y valores permitidos de la clasificación."""
    if raw.get("es_valido") is False:
        motivo = str(raw.get("motivo") or "El clasificador marcó el reporte como ambiguo.")
        return ResultadoClasificacion(
            categoria=None,
            prioridad=None,
            resumen=None,
            es_valido=False,
            motivo=motivo.strip(),
        )

    categoria = raw.get("categoria")
    if not isinstance(categoria, str) or not categoria.strip():
        return ResultadoClasificacion(
            None, None, None, False, "La 'categoria' debe ser un string no vacío."
        )
    categoria_norm = categoria.strip().lower()
    if categoria_norm not in CATEGORIAS_PERMITIDAS:
        return ResultadoClasificacion(
            None,
            None,
            None,
            False,
            f"Categoría '{categoria_norm}' fuera del conjunto permitido: {sorted(CATEGORIAS_PERMITIDAS)}.",
        )

    prioridad = raw.get("prioridad")
    # En Python isinstance(True, int) es True; excluimos booleanos explícitamente.
    if isinstance(prioridad, bool) or not isinstance(prioridad, int) or not (1 <= prioridad <= 5):
        return ResultadoClasificacion(
            None,
            None,
            None,
            False,
            "La 'prioridad' debe ser un entero entre 1 y 5 (no booleano).",
        )

    resumen = raw.get("resumen")
    if not isinstance(resumen, str) or not resumen.strip():
        return ResultadoClasificacion(
            None, None, None, False, "El 'resumen' debe ser un string no vacío."
        )

    return ResultadoClasificacion(
        categoria=categoria_norm,
        prioridad=prioridad,
        resumen=resumen.strip(),
        es_valido=True,
        motivo=str(raw.get("motivo") or "Clasificación válida.").strip(),
    )


def _clasificador_fallback(payload: dict[str, Any]) -> dict[str, Any]:
    """Clasificador de respaldo basado en reglas cuando no hay API key o falla la IA."""
    titulo = str(payload.get("titulo") or "").strip()
    descripcion = str(payload.get("descripcion") or "").strip()
    ubicacion = str(payload.get("ubicacion") or "").strip()
    severidad = str(payload.get("severidad_sugerida") or "").strip().lower()

    if not titulo or not descripcion:
        return {
            "es_valido": False,
            "motivo": "Faltan campos obligatorios ('titulo' o 'descripcion').",
        }

    if not ubicacion or severidad not in MAPA_SEVERIDAD_FALLBACK:
        return {
            "es_valido": False,
            "motivo": "Reporte ambiguo o incompleto: falta 'ubicacion' o 'severidad_sugerida' válida.",
        }

    texto = f"{titulo} {descripcion}".lower()
    if any(k in texto for k in ("agua", "tubería", "luz", "transformador", "alcantarilla")):
        categoria = "infraestructura"
    elif any(k in texto for k in ("acceso", "login", "permiso", "clave")):
        categoria = "acceso"
    elif any(k in texto for k in ("sistema", "servicio", "turno", "despacho")):
        categoria = "operaciones"
    else:
        categoria = "otro"

    return {
        "categoria": categoria,
        "prioridad": MAPA_SEVERIDAD_FALLBACK[severidad],
        "resumen": f"{titulo} en {ubicacion}: {descripcion}",
        "es_valido": True,
        "motivo": "Clasificado con modo fallback por reglas.",
    }


def _llamar_openai(payload: dict[str, Any], api_key: str) -> dict[str, Any]:
    """Llama a la API de OpenAI solicitando JSON estricto."""
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    prompt = (
        "Clasifica el siguiente reporte ciudadano/operativo y responde SOLO con un JSON válido "
        "con las claves: categoria (una de: infraestructura, operaciones, acceso, otro), "
        "prioridad (entero 1 a 5), resumen (string corto), es_valido (boolean) y motivo (string). "
        "Si el reporte es ambiguo o le falta ubicación/contexto claro, devuelve es_valido=false.\n\n"
        f"Reporte:\n{json.dumps(payload, ensure_ascii=False)}"
    )
    body = json.dumps(
        {
            "model": model,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise ValueError("La respuesta del modelo no es un objeto JSON.")
    return parsed


def clasificar_reporte(payload: dict[str, Any]) -> ResultadoClasificacion:
    """Clasifica un payload parseado usando IA (si hay API key) o fallback, y valida la salida."""
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if not api_key:
        raw = _clasificador_fallback(payload)
        return validar_salida_clasificacion(raw)

    try:
        raw = _llamar_openai(payload, api_key)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, KeyError, ValueError) as exc:
        # Si la llamada externa falla o devuelve JSON malformado, no se rompe el lote:
        # usamos el fallback local o marcamos para revisión según el contenido.
        fallback_raw = _clasificador_fallback(payload)
        if not fallback_raw.get("es_valido"):
            fallback_raw["motivo"] = f"Fallo IA ({exc}) y fallback marcó revisión."
        return validar_salida_clasificacion(fallback_raw)

    return validar_salida_clasificacion(raw)
