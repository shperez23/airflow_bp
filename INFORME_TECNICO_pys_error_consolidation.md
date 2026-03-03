# Informe técnico: `pys_error_consolidation.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_error_consolidation.py`
- **Propósito del módulo:** unificar errores de upload/discovery/read en un esquema canónico único.
- **Rol dentro del pipeline/arquitectura:** capa de **observabilidad transversal** de errores.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| include_skipped | bool | N/D | Incluye estados `SKIPPED*` como eventos de error operativo | Opcional | `True` |
| deduplicate_errors | bool | N/D | Elimina duplicados por claves de error | Opcional | `True` |

### 3) DATOS DE ENTRADA
- **Fuente de datos:** DFs de `pys_upload`, `pys_discovery_node`, `pys_read_normalize_node`.
- **Esquema esperado (columnas relevantes):**
  - upload: `full_path`, `status`, `error_message`
  - discovery: `discovery_status`, `source_file`, `path`, `error_message`
  - read: `record_status`, `error_stage`, `source_file`, `path`, `dataset`, `batch_id`, `error_message`
- **Validaciones aplicadas:** detección dinámica de contrato por columnas.
- **Supuestos técnicos:** los nodos upstream preservan convenciones de status (`ERROR*`, `SKIPPED*`).

### 4) FLUJO DE TRANSFORMACIÓN
1. **Resolver entradas multi-etapa**
   - Qué hace: identifica DFs por nombre de nodo o por contrato de columnas.
   - Qué transforma: entradas heterogéneas -> referencias por etapa.
   - Qué devuelve: `upload_df`, `discovery_df`, `read_df`.
   - Dependencias internas: `hasattr(get)` + inferencia de columnas.
2. **Mapear contratos a esquema canónico**
   - Qué hace: aplica adaptadores por etapa.
   - Qué transforma: columnas heterogéneas -> columnas estándar de error.
   - Qué devuelve: DFs de error homogenizados.
   - Dependencias internas: `map_upload_errors`, `map_discovery_errors`, `map_read_errors`.
3. **Consolidar y estampar timestamp**
   - Qué hace: `unionByName` de errores de todas las etapas.
   - Qué transforma: múltiples DFs -> DF consolidado.
   - Qué devuelve: DF con `error_consolidation_ts`.
   - Dependencias internas: `current_timestamp`.
4. **Deduplicar y proyectar esquema final**
   - Qué hace: `dropDuplicates` opcional por llaves semánticas.
   - Qué transforma: eventos repetidos -> únicos.
   - Qué devuelve: salida canónica de errores.
   - Dependencias internas: `deduplicate_errors`.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Adapter**: normalización de contratos por etapa.
- **Observer/Collector**: vista unificada de eventos de error del flujo.
- **Parameter Guard**: configuración de granularidad (skipped/dedup).

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** inferencia de contrato y filtros por prefijo de estado.
- **Excepciones controladas:** no realiza try/catch amplio; privilegia rutas nulas cuando falta una etapa.
- **Estrategia de logging:** no logger; salida estructurada para explotación downstream.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark canónico de errores.
- **Estructura esperada:** `error_consolidation_ts`, `flow_stage`, `error_code`, `error_message`, `source_file`, `path`, `dataset`, `batch_id`, `error_origin`.
- **Destino:** monitoreo, auditoría, tableros operativos.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** solo Spark SQL functions.
- **Complejidad aproximada:** O(n) sobre suma de errores por etapa.
- **Posibles mejoras:** tipificar catálogo de códigos de error por etapa.
- **Riesgos técnicos:** considerar `SKIPPED` como error puede inflar métricas si no se segmenta.

---
