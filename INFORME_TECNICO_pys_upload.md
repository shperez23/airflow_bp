# Informe técnico: `pys_upload.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_upload.py`
- **Propósito del módulo:** extraer archivos desde SFTP, validar readiness/idempotencia, cargar a S3 raw y generar dataset de resultado de carga.
- **Rol dentro del pipeline/arquitectura:** etapa de **ingesta externa (landing)** y creación de checkpoint técnico de upload.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| SFTP_HOST | string | `10.0.0.63` | Host SFTP origen | Obligatorio | N/A |
| SFTP_PORT | int | `225` | Puerto SFTP | Opcional | `22` |
| SFTP_VAULT_NAME | string | `bdadev-generic-user-sftp-custom` | Alias para resolver credenciales (`spark.db.<vault>.user/pass`) | Obligatorio | N/A |
| NOMBRE_ARCHIVO | string | `RESUMEN.zip` | Nombre base o plantilla para filtro de archivos esperados | Obligatorio | N/A |
| FECHA_DESDE | string (yyyy-MM-dd) | `YYYY-MM-DD` | Inicio de ventana para generar nombres diarios esperados | Opcional | `YYYY-MM-DD` |
| FECHA_HASTA | string (yyyy-MM-dd) | `YYYY-MM-DD` | Fin de ventana para generar nombres diarios esperados | Opcional | `YYYY-MM-DD` |
| SFTP_PATH | string | `/Externo/sperezpe/motor_ingesta` | Raíz remota para discovery recursivo | Obligatorio | N/A |
| BUCKET_BLOB | string | `s3-lagodatos-noprod-03` | Bucket destino raw/quarantine/checkpoint | Obligatorio | N/A |
| relative_upload_file_path | string | `data/sftp1/ESTRUCTURA_ORIGINAL` | Prefijo para zonas `original/`, `uncompressed/`, `quarantine/` | Obligatorio | N/A |
| relative_upload_control_path | string | `data/sftp1/control` | Prefijo de control/checkpoint de upload | Obligatorio | N/A |
| max_concurrency | int | `8` | Concurrencia de `boto3` multipart upload | Opcional | `5` |
| use_threads | bool | `True` | Habilita hilos en transferencia S3 | Opcional | `True` |
| multipart_chunksize_mb | int | `64` | Tamaño de parte multipart (MB) | Opcional | `64` |
| multipart_threshold_mb | int | `64` | Umbral para activar multipart (MB) | Opcional | `64` |
| readiness_wait_seconds | int | N/D | Espera entre dos `stat()` para detectar copia activa | Opcional | `2` |
| readiness_skip_wait_age_seconds | int | N/D | Omite espera si archivo remoto tiene antigüedad suficiente | Opcional | `30` |

### 3) DATOS DE ENTRADA
- **Fuente de datos:**
  - DataFrame de parametría (`tri_parametros_upload`).
  - Repositorio de archivos remoto SFTP.
  - Checkpoint parquet en S3 (`.../checkpoints/files/`).
- **Esquema esperado (columnas relevantes):** `SFTP_HOST`, `SFTP_PORT`, `SFTP_VAULT_NAME`, `NOMBRE_ARCHIVO`, `FECHA_DESDE`, `FECHA_HASTA`, `SFTP_PATH`, `BUCKET_BLOB`.
- **Validaciones aplicadas:** no vacío para parámetros críticos; validación de formato/orden de fechas; existencia de credenciales en runtime; validación de readiness por tamaño/mtime; deduplicación por hash y firma metadata.
- **Supuestos técnicos:** credenciales S3 disponibles en Spark conf; conectividad SFTP; permisos de escritura en bucket destino.

### 4) FLUJO DE TRANSFORMACIÓN
1. **Resolver parámetros y credenciales**
   - Qué hace: lee primera fila de parametría y `param_dict`; obtiene credenciales por vault.
   - Qué transforma: normaliza bool/int y defaults.
   - Qué devuelve: contexto de ejecución validado.
   - Dependencias internas: `get_int_param`, `get_bool_param`, `get_row_value`.
2. **Descubrir y filtrar archivos candidatos**
   - Qué hace: recorre SFTP recursivo; filtra por estrategia de nombre/ventana de fechas.
   - Qué transforma: listado remoto crudo -> candidatos esperados.
   - Qué devuelve: rutas candidatas a descarga.
   - Dependencias internas: `list_files_recursive`, `build_expected_filenames`.
3. **Aplicar idempotencia/readiness y subir a S3**
   - Qué hace: compara firma metadata/hash contra checkpoint; valida estabilidad; sube raw y opcional descompresión.
   - Qué transforma: archivo remoto -> objeto S3 raw + (opcional) uncompressed.
   - Qué devuelve: registros de estado por archivo.
   - Dependencias internas: `file_hash`, `TransferConfig`, `boto3`, `gzip`.
4. **Persistir checkpoint y emitir salida**
   - Qué hace: escribe metadatos de archivos nuevos en parquet checkpoint.
   - Qué transforma: lista `checkpoint_records` -> parquet de control.
   - Qué devuelve: DataFrame final (`full_path`, `s3_key`, `status`, `error_stage`, `error_message`).
   - Dependencias internas: Spark DataFrame writer.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Parameter Guard**
  - Dónde: helpers de parseo al inicio.
  - Por qué: robustez frente a nulos/tipos variables del orquestador.
  - Beneficio: evita fallos por contrato débil de entrada.
  - Riesgo si no se usara: excepciones tempranas por casting/None.
- **Secrets Pointer**
  - Dónde: resolución `spark.db.<vault>.user/pass`.
  - Por qué: desacoplar credenciales del código.
  - Beneficio: seguridad y portabilidad.
  - Riesgo: hardcode de secretos.
- **Idempotent Consumer + Checkpointer**
  - Dónde: sets `processed_hashes`, `processed_signatures` + parquet checkpoint.
  - Por qué: evitar reproceso y duplicados.
  - Beneficio: consistencia de carga incremental.
  - Riesgo: duplicación, sobrecostos y corrupción lógica.
- **Dead-Letter/Quarantine**
  - Dónde: bloque `except` por archivo con upload a `quarantine`.
  - Por qué: aislar fallas conservando evidencia.
  - Beneficio: trazabilidad operativa.
  - Riesgo: pérdida de artefacto fallido.

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** contrato de parámetros, fechas, credenciales, readiness, extensión/compresión.
- **Excepciones controladas:** `ValueError` de contrato; excepciones de I/O SFTP/S3 comprimidas en `status` de salida; fallback silencioso al leer checkpoint (`except Exception: pass`).
- **Estrategia de logging:** no usa logger estructurado; reporta errores en columnas `status/error_stage/error_message`.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark de auditoría de upload.
- **Estructura esperada:** `full_path`, `s3_key`, `status`, `error_stage`, `error_message`.
- **Destino:** nodo downstream (`pys_discovery_node`) y checkpoint parquet en S3.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** `pysftp`, `boto3`, `gzip`, S3A runtime.
- **Complejidad aproximada:** O(n) por archivos descubiertos; O(size) para hash por archivo.
- **Posibles mejoras:** reemplazar `except:` genéricos; agregar logging estructurado con correlación; desacoplar ruta `/tmp` para entornos restringidos.
- **Riesgos técnicos:** `cnopts.hostkeys=None` deshabilita validación host key; captura amplia de excepciones oculta fallos de infraestructura.

---
