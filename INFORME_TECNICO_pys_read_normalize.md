# Informe técnico: `pys_read_normalize.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_read_normalize.py`
- **Propósito del módulo:** leer archivos pendientes multi-formato, expandir ZIP, normalizar esquema a string y adjuntar metadatos técnicos.
- **Rol dentro del pipeline/arquitectura:** etapa de **lectura + estandarización** antes de escritura curada.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| READER_OPTIONS | JSON/dict | `{"csv":...,"txt":...,"json":...,"parquet":{},"excel":...}` | Configuración de lectura por extensión | Obligatorio | N/A |
| BUCKET_BLOB | string | `s3-lagodatos-noprod-03` | Bucket para staging de archivos internos de ZIP | Obligatorio (cuando hay ZIP) | N/A |
| relative_upload_file_path | string | `data/sftp1/ESTRUCTURA_ORIGINAL` | Prefijo para staging ZIP descomprimido | Obligatorio (cuando hay ZIP) | N/A |
| poi_max_byte_array_size | int | N/D | Override runtime Apache POI para Excel grande | Opcional | N/A |

### 3) DATOS DE ENTRADA
- **Fuente de datos:** salida de `pys_discovery_node` (`path`, `discovery_status`, `source_file`) y parametría `tri_parametros_read`.
- **Esquema esperado (columnas relevantes):** columna `path` obligatoria; opcionales `discovery_status`, `error_message`, `source_file`.
- **Validaciones aplicadas:** JSON válido en `READER_OPTIONS`; extensión soportada; existencia de opciones por extensión; control de ZIP sin archivos legibles.
- **Supuestos técnicos:** conectividad S3 para staging ZIP; conector `com.crealytics.spark.excel` disponible para Excel.

### 4) FLUJO DE TRANSFORMACIÓN
1. **Resolver input y contrato de lectura**
   - Qué hace: identifica DF de discovery y parsea `READER_OPTIONS`.
   - Qué transforma: entrada heterogénea -> contrato canónico (`path/status/source_file`).
   - Qué devuelve: `files` pendientes + errores heredados.
   - Dependencias internas: `resolve_input_frame`, `get_param_read_value`.
2. **Expandir rutas comprimidas (si aplica)**
   - Qué hace: maneja `.zip` (expansión a staging S3) y passthrough en otros comprimidos.
   - Qué transforma: `path` contenedor -> múltiples `read_path` internos.
   - Qué devuelve: lista de targets trazables.
   - Dependencias internas: `expand_input_paths`, `build_s3_client_from_runtime`.
3. **Lectura dinámica por extensión y normalización**
   - Qué hace: lee csv/txt/json/parquet/excel usando opciones por tipo.
   - Qué transforma: datasets heterogéneos -> columnas string + metadatos técnicos.
   - Qué devuelve: DFs por dataset con `record_status=PROCESADO`.
   - Dependencias internas: `read_dynamic`, `cast_all_to_string`, `dataset_name`.
4. **Consolidar salida y errores**
   - Qué hace: union por dataset y unión adicional con errores heredados/lectura.
   - Qué transforma: múltiples DFs/eventos -> DF único.
   - Qué devuelve: DataFrame normalizado para writer y summary.
   - Dependencias internas: `unionByName`, `error_records`.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Strategy (Reader Options Injection)**: selección dinámica de lector por extensión.
- **Adapter (Input Contract Resolver)**: adapta contratos upstream variados.
- **Decorator (Metadata enrichment)**: agrega `ingestion_ts`, `batch_id`, `dataset`, `record_status`.
- **Fail-soft error propagation**: errores se registran en filas, no abortan lote completo.

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** parámetros, formato JSON, extensiones soportadas, lectura ZIP/Excel.
- **Excepciones controladas:** errores de parseo/lectura se convierten en registros `ERROR_READ`; fallback en Excel (`inferSchema=false` en reintento).
- **Estrategia de logging:** sin logger externo; observabilidad mediante filas de error con `error_stage=READ_NORMALIZE`.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark normalizado.
- **Estructura esperada:** columnas de negocio (string) + `ingestion_ts`, `source_file`, `path`, `batch_id`, `dataset`, `record_status`, `error_stage`, `error_message`.
- **Destino:** `pys_write_curated`, `pys_ingestion_summary`, `pys_error_consolidation`.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** `boto3`, `zipfile`, `com.crealytics.spark.excel`.
- **Complejidad aproximada:** O(n archivos + n miembros ZIP + tamaño datos leídos).
- **Posibles mejoras:** evitar `collect()` de todos los paths en lotes grandes (usar particionado/foreachPartition).
- **Riesgos técnicos:** staging ZIP puede generar objetos temporales sin política de limpieza explícita.

---
