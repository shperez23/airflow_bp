# Informe técnico: `pys_dicovery_node.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_dicovery_node.py` *(typo en nombre: falta “s” en discovery)*.
- **Propósito del módulo:** construir lista de archivos pendientes para lectura, a partir de upload o discovery standalone, excluyendo ya procesados por checkpoint.
- **Rol dentro del pipeline/arquitectura:** etapa de **orquestación de pendientes** entre upload y read/normalize.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| BUCKET_BLOB | string | `s3-lagodatos-noprod-03` | Bucket para resolver `s3_key -> s3a://...` | Obligatorio | N/A |
| BUCKET_RAW | string | `s3-lagodatos-noprod-01` | Bucket donde vive checkpoint de archivos escritos | Obligatorio | N/A |
| relative_upload_file_path | string | `data/sftp1/ESTRUCTURA_ORIGINAL` | Prefijo para modo standalone | Condicional | N/A |
| checkpoint_prefix | string | `data/control/conversion_checkpoint/` | Prefijo parquet checkpoint writer/read | Obligatorio | N/A |
| include_upload_errors | bool | N/D | Propaga errores de upload hacia downstream | Opcional | `True` |
| include_upload_skipped | bool | N/D | Incluye eventos SKIPPED en salida de discovery | Opcional | `False` |

### 3) DATOS DE ENTRADA
- **Fuente de datos:**
  - DataFrame `pys_upload` (contrato: `full_path`, `s3_key`, `status`) o
  - lectura directa `binaryFile` sobre `s3a://<BUCKET_BLOB>/<relative_upload_file_path>`.
  - DataFrame parametría `tri_parametros_discovery`.
  - Checkpoint parquet de `pys_write_curated`.
- **Esquema esperado:** `BUCKET_BLOB`, `BUCKET_RAW`; opcional `pys_upload` con columnas de estado.
- **Validaciones aplicadas:** presencia de bucket/prefijos; validación de contrato de columnas; anti-join contra checkpoint.
- **Supuestos técnicos:** checkpoint accesible en S3A; compatibilidad con contrato antiguo/nuevo de checkpoint (`path`/`source_file`).

### 4) FLUJO DE TRANSFORMACIÓN
1. **Resolver entradas multi-nodo**
   - Qué hace: detecta DataFrame de upload y parametría discovery.
   - Qué transforma: diccionario de entradas -> `upload_df` + `param_discovery_row`.
   - Qué devuelve: contexto de entradas usable.
   - Dependencias internas: `resolve` implícito por `hasattr/get`.
2. **Construir candidatos pendientes**
   - Qué hace: desde `pys_upload` (PROCESADO) o desde `binaryFile` standalone.
   - Qué transforma: `s3_key`/paths -> columna canónica `path` + `discovery_status`.
   - Qué devuelve: DataFrame `pending`.
   - Dependencias internas: `has_upload_contract`, `selectExpr`.
3. **Filtrar ya procesados por checkpoint**
   - Qué hace: lee parquet checkpoint y adapta contrato viejo/nuevo.
   - Qué transforma: `pending` -> `pending left_anti processed`.
   - Qué devuelve: pendientes reales.
   - Dependencias internas: `checkpoint_prefix`, `bucket_raw`, `join left_anti`.
4. **Unificar con errores de upload (opcional)**
   - Qué hace: une errores/skipped según flags.
   - Qué transforma: eventos error + pendientes.
   - Qué devuelve: DataFrame final para `pys_read_normalize`.
   - Dependencias internas: `include_upload_errors`, `include_upload_skipped`.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Multi-Input Resolver**: selección dinámica de origen según orquestador.
- **Contract Detection/Adapter**: detección de contrato upload y adaptación de checkpoint legacy.
- **Checkpointer (left_anti)**: control incremental sin reproceso.
- **Fallback Standalone Source**: operación incluso sin nodo upload.

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** parámetros requeridos, contratos de columnas.
- **Excepciones controladas:** `ValueError` por faltantes; lectura de checkpoint con `except Exception` que degrada a `distinct`.
- **Estrategia de logging:** no logger dedicado; propagación en columnas `discovery_status/error_message`.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark de paths pendientes y errores heredados.
- **Estructura esperada:** `path`, `discovery_status`, `error_message`, `source_file`.
- **Destino:** `pys_read_normalize`.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** Spark `binaryFile`, S3A.
- **Complejidad aproximada:** O(n) en candidatos + costo de join con checkpoint.
- **Posibles mejoras:** renombrar archivo a `pys_discovery_node.py`; tipar/normalizar estado en enum.
- **Riesgos técnicos:** catch-all en checkpoint puede ocultar corrupción de metadatos.

---
