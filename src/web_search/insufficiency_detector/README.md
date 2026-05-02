# Insufficiency Detector: diseño y funcionamiento interno

Este módulo decide si la evidencia recuperada localmente (BM25 + vector + fusión) es suficiente para responder una consulta, o si se debe activar búsqueda web externa.

Su salida no genera respuesta final: actúa como un **gate de suficiencia** antes de llamar al LLM.

## 1) Ubicación en la arquitectura RAG

Flujo simplificado en `RAGPipeline.query()`:

1. Recupera candidatos con `HybridRetriever`.
2. (Opcional) Re-rank con cross-encoder.
3. Construye `RetrievedChunk` para cada chunk final.
4. Ejecuta `InsufficiencyDetector.evaluate(...)`.
5. Si `needs_web_search=True`, retorna respuesta de corte: `"need web search"`.
6. Si `False`, continúa a prompt + LLM.

En código, este gate está integrado en:

- `src/generation/rag_pipeline.py`
- `src/web_search/insufficiency_detector/detector.py`

## 2) Contratos de datos

### Entrada principal al detector

`evaluate(query, results, retrieval_context=None)`:

- `query: str`: pregunta del usuario.
- `results: list[RetrievedChunk]`: evidencia textual ya enriquecida con score y metadata.
- `retrieval_context: Mapping[str, Any] | None`: contexto opcional del método de fusión (por ejemplo RRF) para normalizar score.

`RetrievedChunk` (`schemas.py`) contiene:

- Identidad: `chunk_id`
- Texto: `text`
- Señales de ranking: `score`
- Señales de origen: `source_id`, `url`
- Metadata de presentación: `title`, `breadcrumb`, `metadata`

### Salida

`InsufficiencyDecision`:

- `needs_web_search: bool`
- `sufficiency_confidence: float` en `[0, 1]`
- `reasons: list[InsufficiencyReason]` (explicabilidad)
- `metrics: InsufficiencyMetrics` (telemetría detallada)

`InsufficiencyReason` (`reasons.py`) enumera:

- `NO_RESULTS`
- `LOW_NUM_RESULTS`
- `LOW_TOP_SCORE`
- `LOW_COVERAGE`
- `LOW_SOURCE_DIVERSITY`
- `LOW_ANSWERABILITY`
- `LOW_CONFIDENCE`

## 3) Pipeline algorítmico de `evaluate()`

El método combina señales heterogéneas (cantidad, score, cobertura lexical, diversidad y answerability) en una confianza local.

### Paso A: short-circuit de cero resultados

Si `len(results) == 0`:

- fuerza métricas a `0.0`
- retorna `needs_web_search=True`
- reasons = `[NO_RESULTS]`

Esto evita cálculos ambiguos y define un comportamiento determinista de fallback.

### Paso B: métricas base

Con resultados no vacíos:

1. `top_score = max(score)` ignorando `None` (`_max_non_none`).
2. `top_score_norm = _normalize_top_score(top_score, retrieval_context)`.
3. `quantity_score = min(1.0, num_results / expected_results)`.
4. `coverage_score, relevant_results = _coverage_metrics(query, results)`.
5. `unique_urls = _unique_url_count(results)`.
6. `diversity_score = unique_urls / num_results`.
7. `answerability_score = _answerability_score(coverage_score, relevant_results)`.

Todas las métricas continuas se saturan a `[0, 1]` vía `_clamp01`.

### Paso C: agregación ponderada de confianza

Se calcula:

```text
local_confidence =
    w_top          * top_score_norm
  + w_quantity     * quantity_score
  + w_coverage     * coverage_score
  + w_diversity    * diversity_score
  + w_answerability* answerability_score
```

Luego se aplica clamp `[0,1]`.

Los pesos (`w_*`) se validan en configuración para sumar exactamente `1.0` (tolerancia `1e-6`).

### Paso D: reasons por umbrales

Se agregan reasons independientes por métrica:

- `LOW_NUM_RESULTS` si `num_results < min_results`
- `LOW_TOP_SCORE` si `top_score_norm < min_top_score`
- `LOW_COVERAGE` si `coverage_score < min_coverage_score`
- `LOW_SOURCE_DIVERSITY` si `diversity_score < min_source_diversity`
- `LOW_ANSWERABILITY` si `answerability_score < min_answerability_score`

Decisión final:

- `needs_web_search = local_confidence < confidence_threshold`
- si se activa, agrega `LOW_CONFIDENCE`

Finalmente deduplica reasons preservando orden (`_dedupe_preserve_order`).

## 4) Tokenización y cobertura semántica superficial

La cobertura es lexical (no embedding-based):

- tokenizador: `simple_tokenize`
- regex: `r"[^\W_]+"` (Unicode words, excluye `_`)
- minúsculas
- filtro de stopwords EN+ES de alto impacto
- descarta tokens de longitud `<= 1`

Esto permite conservar acentos y `ñ` en español (validado por test).

### Cálculo de cobertura

`_coverage_metrics(query, results)`:

1. Tokeniza query y crea `query_terms`.
2. Ordena resultados por score descendente (independiente del orden de entrada).
3. Toma `top_n = min(len(results), coverage_top_n)`.
4. Por chunk calcula:

```text
overlap_i = |query_terms ∩ chunk_terms| / |query_terms|
```

5. `coverage_score` = promedio de `overlap_i` en los `top_n`.
6. `relevant_results` = count de chunks con `overlap_i >= relevant_overlap_threshold`.

Observación: cobertura mide presencia de términos de query, no fidelidad factual ni completitud multi-hop.

## 5) Score normalization para fusión RRF

Cuando `retrieval_context["fusion"]["method"] == "rrf"`:

1. Lee `rrf_k` (default `60`).
2. Suma pesos de listas (`weights`) si existen.
3. Estima score máximo teórico del doc rank-1 en todas las listas:

```text
max_rrf = weight_sum / (rrf_k + 1)
```

4. Normaliza:

```text
top_score_norm = clamp(top_score / max_rrf)
```

Si el contexto no es RRF (o no usable), usa `top_score_norm = clamp(top_score)`.

Esto hace comparable la señal de top-score aun cuando la métrica upstream sea RRF y no probabilidad calibrada.

## 6) Answerability: combinación de profundidad + anchura

`_answerability_score` mezcla:

- **profundidad**: cuántos chunks relevantes hay (`relevant_results`)
- **anchura**: cobertura media (`coverage_score`)

Definición:

```text
relevant_ratio = min(1.0, relevant_results / min_relevant_results)
answerability = clamp(0.6 * relevant_ratio + 0.4 * coverage_score)
```

Si `min_relevant_results <= 0`, fuerza `relevant_ratio = 1.0`.

Interpretación: prioriza tener suficientes evidencias “útiles” sobre cobertura promedio.

## 7) Diversidad de fuentes

`_unique_url_count`:

- prioridad a `url` (si existe)
- fallback a `source_id` cuando URL no está

`diversity_score = unique_urls / num_results`

Favorece evitar redundancia de chunks provenientes de una sola página/fuente.

## 8) Configuración (`INSUFF_*`) y defaults

`load_settings()`:

- resuelve root del proyecto
- garantiza existencia de `.env`
- inserta claves faltantes con `ENV_DEFAULTS`
- carga env sin sobreescribir variables ya definidas del proceso
- parsea y valida rangos

Variables:

- Conteo: `INSUFF_MIN_RESULTS`, `INSUFF_EXPECTED_RESULTS`
- Score: `INSUFF_MIN_TOP_SCORE`
- Cobertura/answerability:
  - `INSUFF_RELEVANT_OVERLAP_THRESHOLD`
  - `INSUFF_MIN_RELEVANT_RESULTS`
  - `INSUFF_MIN_COVERAGE_SCORE`
  - `INSUFF_MIN_ANSWERABILITY_SCORE`
- Diversidad: `INSUFF_MIN_SOURCE_DIVERSITY`
- Decisión global: `INSUFF_CONFIDENCE_THRESHOLD`
- Límite de cómputo: `INSUFF_COVERAGE_TOP_N`
- Pesos:
  - `INSUFF_W_TOP`
  - `INSUFF_W_QUANTITY`
  - `INSUFF_W_COVERAGE`
  - `INSUFF_W_DIVERSITY`
  - `INSUFF_W_ANSWERABILITY`

Validaciones:

- enteros positivos para conteos y `top_n`
- fracciones en `[0,1]` para umbrales y pesos
- suma de pesos == `1.0`

## 9) Integración exacta con el RAG actual

El pipeline envía `retrieval_context` con:

- `method = "rrf"`
- `rrf_k = 60`
- `weights = {"bm25": _bm25_weight, "vector": _vector_weight}`

Si el detector marca insuficiencia:

- evita llamar al LLM
- retorna `RAGResult` con:
  - `answer = "need web search"`
  - `model = "web-search-gate"`
  - tokens en `0`

También emite log estructurado con:

- `needs_web_search`
- `confidence`
- `reasons`
- `metrics`

Esto facilita tuning por observabilidad.

## 10) Propiedades y garantías observadas en tests

Cubierto por pruebas unitarias/integración:

- caso sin resultados fuerza `NO_RESULTS`
- normalización RRF puede alcanzar `1.0` en condiciones máximas
- baja diversidad agrega `LOW_SOURCE_DIVERSITY`
- baja cobertura dispara `LOW_COVERAGE` y `LOW_ANSWERABILITY`
- independencia del orden de entrada para top-score/cobertura (se ordena por score)
- tokenización preserva acentos y `ñ`
- en pipeline, gate bloquea LLM cuando requiere web search

## 11) Trade-offs y limitaciones técnicas

1. Cobertura lexical:
   no capta sinónimos/paráfrasis fuera de intersección de tokens.
2. Stopwords fijas:
   pueden no ajustarse a dominios muy especializados.
3. Diversidad por URL/source_id:
   no mide diversidad semántica real.
4. Score normalization parcial:
   sólo trata RRF explícitamente; otros métodos dependen de escala upstream.
5. Umbrales globales:
   no están calibrados por tipo de consulta (factoid vs multi-hop).

## 12) Guía de tuning práctico

Recomendación operativa:

1. Registrar distribución de `metrics` y `reasons` en tráfico real.
2. Separar consultas donde:
   - el LLM respondió bien sin web
   - el detector habría pedido web
3. Ajustar en este orden:
   - `confidence_threshold` (sensibilidad global)
   - pesos `w_*` (importancia relativa de señales)
   - umbrales por señal (`min_*`)
4. Revalidar con tests y set de queries etiquetadas.

Heurística útil:

- muchos falsos positivos de web-search:
  bajar `confidence_threshold` o subir peso de señales robustas en tu dominio.
- muchos falsos negativos (respuestas malas locales):
  subir `confidence_threshold`, `min_coverage_score` y/o `min_answerability_score`.

## 13) API pública del módulo

Exportada en `src/web_search/insufficiency_detector/__init__.py`:

- `InsufficiencyDetector`
- `InsufficiencyDetectorSettings`
- `InsufficiencyDecision`
- `InsufficiencyMetrics`
- `InsufficiencyReason`
- `RetrievedChunk`
- `load_settings()`

Esto permite reuso desacoplado en otras rutas/pipelines sin depender de implementación interna.
