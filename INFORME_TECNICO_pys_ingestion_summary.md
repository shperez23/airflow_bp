# Informe técnico: `pys_ingestion_summary.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_ingestion_summary.py`
- **Propósito del módulo:** generar métricas de ingesta por dataset/batch y checksums de contenido fuente.
- **Rol dentro del pipeline/arquitectura:** capa de **auditoría cuantitativa** previa/post escritura.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| include_global_summary | bool | N/D | Agrega fila GLOBAL agregada (snapshot del lote) | Opcional | `False` |
| use_source_checksum_as_destination | bool | N/D | Replica checksum fuente en destino (modo espejo temporal) | Opcional | `False` |

### 3) DATOS DE ENTRADA
- **Fuente de datos:** salida de `pys_read_normalize` (o dataset compatible).
- **Esquema esperado (columnas relevantes):** `dataset`, `batch_id`, `source_file`, `record_status`, `error_message` + columnas de negocio.
- **Validaciones aplicadas:** completa columnas faltantes con defaults para tolerancia contractual.
- **Supuestos técnicos:** columnas técnicas no forman parte del checksum de negocio.

### 4) FLUJO DE TRANSFORMACIÓN
1. **Normalizar contrato de entrada**
   - Qué hace: garantiza presencia de columnas técnicas mínimas.
   - Qué transforma: entrada parcial -> contrato summary consistente.
   - Qué devuelve: DF enriquecido con defaults.
   - Dependencias internas: `withColumn` condicional.
2. **Calcular fingerprint por fila**
   - Qué hace: identifica columnas de negocio y calcula `row_checksum`.
   - Qué transforma: valores de negocio -> hash por fila.
   - Qué devuelve: DF con checksum técnico.
   - Dependencias internas: `concat_ws`, `hash`, `coalesce`.
3. **Agregar métricas por dataset/batch**
   - Qué hace: cuenta totales/leídos/insertados/error/archivos y checksum agregado.
   - Qué transforma: granularidad fila -> resumen agregado.
   - Qué devuelve: DF summary base.
   - Dependencias internas: `groupBy`, `count`, `countDistinct`, `collect_list`.
4. **Completar estado e invariantes de salida**
   - Qué hace: define `checksum_destination` y `estado_ingesta`; agrega fila GLOBAL opcional.
   - Qué transforma: summary base -> summary final.
   - Qué devuelve: DataFrame de indicadores de ingesta.
   - Dependencias internas: flags `include_global_summary/use_source_checksum_as_destination`.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Input Contract Resolver**: resiliencia a contratos parciales.
- **Audit-Write-Audit**: auditoría de integridad/cantidades alrededor del flujo de datos.
- **Aggregate Snapshot**: fila GLOBAL opcional para reporting ejecutivo.

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** no falla por ausencia de columnas no críticas; completa defaults.
- **Excepciones controladas:** no define manejo explícito; delega a runtime Spark.
- **Estrategia de logging:** no logger; salida es dataset de métricas/auditoría.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark de resumen de ingesta.
- **Estructura esperada:** `dataset`, `batch_id`, `cantidad_registros_totales`, `cantidad_registros_leidos`, `cantidad_registros_insertados`, `cantidad_registros_error`, `cantidad_archivos`, `error`, `checksum_source`, `checksum_destination`, `ingestion_summary_ts`, `estado_ingesta`.
- **Destino:** monitoreo de calidad, reconciliación y reporting.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** Spark SQL functions.
- **Complejidad aproximada:** O(n) + costo de agregación por cardinalidad de `dataset/batch`.
- **Posibles mejoras:** checksum destino real post-write (no espejo) para reconciliación E2E.
- **Riesgos técnicos:** `hash` de Spark no es criptográfico; potencial colisión en casos extremos.
