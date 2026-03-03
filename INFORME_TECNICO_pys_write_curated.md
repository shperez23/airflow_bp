# Informe técnico: `pys_write_curated.py`

### 1) INFORMACIÓN GENERAL
- **Nombre del archivo:** `pys_write_curated.py`
- **Propósito del módulo:** materializar dataset en snapshot parquet (`current`) e histórico Delta (`_shist`), y registrar checkpoint de paths escritos.
- **Rol dentro del pipeline/arquitectura:** etapa de **persistencia curada** con modo reprocess controlado.

### 2) PARÁMETROS DE ENTRADA

| Nombre | Tipo | Valor de ejemplo (captura) | Descripción funcional | Obligatorio / Opcional | Valor por defecto |
|---|---|---|---|---|---|
| RUTA_SALIDA | string | `s3a://s3-lagodatos-noprod-01/data/tmp/bp/test` | Raíz de escritura current/history | Obligatorio | N/A |
| NOMBRE_TABLA | string | `ZR_BP_Can_Cash_Tmp_TestIngesta` | Nombre lógico para carpetas destino | Obligatorio | N/A |
| BUCKET_RAW | string | `s3-lagodatos-noprod-01` | Bucket del checkpoint post-write | Opcional (pero requerido para checkpoint) | N/A |
| ENABLE_REPROCESS | bool | N/D | Si `true`, sobrescribe current y limpia history cuando existe | Opcional | `False` |
| checkpoint_prefix | string | `data/control/conversion_checkpoint/` | Prefijo del checkpoint consumido por discovery | Opcional (condicional) | N/A |

### 3) DATOS DE ENTRADA
- **Fuente de datos:** salida `pys_read_normalize_node` y parametría `tri_parametros_write`.
- **Esquema esperado (columnas relevantes):** datos de negocio + `record_status`, `path`, `source_file`, `dataset`, `batch_id`.
- **Validaciones aplicadas:** entrada no nula; `RUTA_SALIDA/NOMBRE_TABLA` obligatorios; intercepción temprana si existen errores upstream.
- **Supuestos técnicos:** soporte Delta Lake habilitado en runtime.

### 4) FLUJO DE TRANSFORMACIÓN
1. **Resolver entradas y validar contrato**
   - Qué hace: identifica input principal y parámetros de write.
   - Qué transforma: entradas múltiples -> contexto de escritura.
   - Qué devuelve: paths de salida y flags operativos.
   - Dependencias internas: `resolve_input_frame`, `get_value_from_param_df`.
2. **Aplicar guardas operativas**
   - Qué hace: aborta escritura si hay `record_status=ERROR`; retorna `SKIPPED_NO_DATA` si no hay registros.
   - Qué transforma: estado de input -> decisión de escritura.
   - Qué devuelve: input original o DF de control skipped.
   - Dependencias internas: `errors_df`, `limit(1).count()`.
3. **Escribir current + history**
   - Qué hace: escribe parquet current y append Delta history.
   - Qué transforma: DF de entrada -> dos persistencias con `write_ts/table_name`.
   - Qué devuelve: artefactos físicos en `current_path/history_path`.
   - Dependencias internas: writer parquet/delta, opcional `DeltaTable.delete`.
4. **Actualizar checkpoint y retornar contrato de salida**
   - Qué hace: registra paths escritos en checkpoint parquet y retorna control de write.
   - Qué transforma: subset no-error -> checkpoint incremental.
   - Qué devuelve: DF con `write_status=WRITTEN`.
   - Dependencias internas: `checkpoint_prefix`, writer parquet.

### 5) PATRONES DE DISEÑO IDENTIFICADOS
- **Error Interceptor**: short-circuit ante errores upstream.
- **Materialized View (Current Snapshot)**: snapshot operativo en parquet.
- **Immutable History (append Delta)**: histórico transaccional para trazabilidad.
- **Checkpointer**: comunicación de “ya escrito” hacia discovery.

### 6) MANEJO DE ERRORES
- **Tipo de validaciones:** parámetros requeridos, estado upstream, payload vacío.
- **Excepciones controladas:** fallback al limpiar history (si falla `DeltaTable.delete`, continúa append).
- **Estrategia de logging:** salida de control (`write_status/write_message`), sin logger estructurado.

### 7) SALIDAS DEL PROCESO
- **Tipo de salida:** DataFrame Spark de control de escritura.
- **Estructura esperada:** `nombre_tabla`, `ruta_current`, `ruta_history_delta`, `enable_reprocess`, `write_status`, `write_message`.
- **Destino:** monitoreo/orquestador; checkpoint para `pys_dicovery_node`.

### 8) OBSERVACIONES TÉCNICAS
- **Dependencias externas:** Delta Lake (`delta.tables`).
- **Complejidad aproximada:** O(n) en volumen de filas para escritura.
- **Posibles mejoras:** reemplazar `.count()` por métodos menos costosos (`head(1)`); validar explícitamente esquema mínimo.
- **Riesgos técnicos:** si no existe Delta runtime, falla etapa history.

---
